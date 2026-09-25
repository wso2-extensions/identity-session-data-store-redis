/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.identity.session.store.redis.util;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.wso2.carbon.identity.session.store.redis.exception.RedisSessionStoreException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Executes Redis commands and scripts on behalf of the store and the DAO, translating every client
 * failure into a {@link RedisSessionStoreException}.
 */
public class RedisTemplate {

    private static final String NO_SCRIPT_ERROR = "NOSCRIPT";
    private static final String SHA_1 = "SHA-1";

    private final RedisConnectionManager connectionManager;
    private final long commandTimeout;
    private final Map<String, String> scriptDigests = new ConcurrentHashMap<>();

    /**
     * Creates a template that runs commands through the given connection manager.
     *
     * @param connectionManager Connection manager of the store.
     * @param commandTimeout    How long a command or a batch is awaited, in milliseconds.
     */
    public RedisTemplate(RedisConnectionManager connectionManager, long commandTimeout) {

        this.connectionManager = connectionManager;
        this.commandTimeout = commandTimeout;
    }

    /**
     * Executes commands against the synchronous command interface.
     *
     * @param callable Commands to execute.
     * @param <T>      Result type.
     * @return the result of the commands.
     * @throws RedisSessionStoreException if the commands could not be executed.
     */
    public <T> T execute(RedisCallable<T> callable) throws RedisSessionStoreException {

        RedisClusterCommands<String, byte[]> commands = connectionManager.getCommands();
        try {
            return callable.execute(commands);
        } catch (RedisException e) {
            throw new RedisSessionStoreException("Error while executing a Redis command.", e);
        }
    }

    /**
     * Executes a script by digest, sending the body only when the target node has not cached it.
     *
     * @param script Script body.
     * @param output Expected reply type.
     * @param key    Single key the script operates on, which is also what routes it in cluster mode.
     * @param args   Script arguments.
     * @param <T>    Reply type implied by {@code output}.
     * @return the reply of the script.
     * @throws RedisSessionStoreException if the script could not be executed.
     */
    public <T> T executeScript(String script, ScriptOutputType output, String key, byte[]... args)
            throws RedisSessionStoreException {

        RedisClusterCommands<String, byte[]> commands = connectionManager.getCommands();
        String[] keys = new String[]{key};
        try {
            return commands.evalsha(getDigest(script), output, keys, args);
        } catch (RedisNoScriptException e) {
            return evaluate(commands, script, output, keys, args);
        } catch (RedisCommandExecutionException e) {
            // If the typed exception, `RedisNoScriptException` is not thrown for some reason.
            if (isScriptNotCached(e)) {
                return evaluate(commands, script, output, keys, args);
            }
            throw new RedisSessionStoreException("Error while executing a Redis script.", e);
        } catch (RedisException e) {
            throw new RedisSessionStoreException("Error while executing a Redis script.", e);
        }
    }

    /**
     * Executes pipelined commands and waits for all of them, so a batch costs a single round trip. A batch is
     * not a transaction, so the commands that did not fail stay applied, and this discards every reply of a
     * batch that holds a failure. Writes whose outcome the caller has to know per command belong in
     * {@link #collectBatch(RedisBatchCallable)} instead.
     *
     * @param callable Commands to issue without awaiting each.
     * @param <T>      Reply type of the issued commands.
     * @return the replies, in the order the commands were issued.
     * @throws RedisSessionStoreException if the commands could not be executed, or if any of them failed.
     */
    public <T> List<T> executeBatch(RedisBatchCallable<T> callable) throws RedisSessionStoreException {

        BatchResult<T> result = collectBatch(callable);
        if (result.hasFailures()) {
            throw new RedisSessionStoreException("A Redis command batch of " + result.getReplies().size()
                    + " commands failed at: " + result.getFailures() + ".");
        }
        return result.getReplies();
    }

    /**
     * Executes pipelined commands the way {@link #executeBatch(RedisBatchCallable)} does, and reports the
     * commands the server rejected rather than losing the batch to the first of them.
     *
     * @param callable Commands to issue without awaiting each.
     * @param <T>      Reply type of the issued commands.
     * @return the replies and the failures, both against the order the commands were issued.
     * @throws RedisSessionStoreException if the commands could not be issued, or if the batch timed out.
     */
    public <T> BatchResult<T> collectBatch(RedisBatchCallable<T> callable) throws RedisSessionStoreException {

        RedisClusterAsyncCommands<String, byte[]> commands = connectionManager.getAsyncCommands();
        List<RedisFuture<T>> pending;
        try {
            pending = callable.issue(commands);
        } catch (RedisException e) {
            throw new RedisSessionStoreException("Error while issuing a Redis command batch.", e);
        }
        return await(pending);
    }

    /**
     * Executes one script against several keys in a single round trip. The body is sent rather than the
     * digest, since a pipelined call cannot recover from a script the node has not cached.
     *
     * @param script Script body.
     * @param output Expected reply type.
     * @param calls  Key and arguments of each invocation.
     * @return the replies, in the order of the calls.
     * @throws RedisSessionStoreException if the scripts could not be executed.
     */
    public List<Object> executeScriptBatch(String script, ScriptOutputType output, List<ScriptCall> calls)
            throws RedisSessionStoreException {

        BatchResult<Object> result = collectScriptBatch(script, output, calls);
        if (result.hasFailures()) {
            throw new RedisSessionStoreException("A Redis script batch of " + result.getReplies().size()
                    + " invocations failed at: " + result.getFailures() + ".");
        }
        return result.getReplies();
    }

    /**
     * Executes a script batch the way {@link #executeScriptBatch(String, ScriptOutputType, List)} does, and
     * reports the invocations the server rejected rather than losing the batch to the first of them.
     *
     * @param script Script body.
     * @param output Expected reply type.
     * @param calls  Key and arguments of each invocation.
     * @return the replies and the failures, both against the order of the calls.
     * @throws RedisSessionStoreException if the scripts could not be issued, or if the batch timed out.
     */
    public BatchResult<Object> collectScriptBatch(String script, ScriptOutputType output, List<ScriptCall> calls)
            throws RedisSessionStoreException {

        if (calls == null || calls.isEmpty()) {
            return new BatchResult<>(Collections.emptyList(), Collections.emptyMap());
        }
        return collectBatch(commands -> {
            List<RedisFuture<Object>> pending = new ArrayList<>(calls.size());
            for (ScriptCall call : calls) {
                pending.add(commands.eval(script, output, new String[]{call.getKey()}, call.args));
            }
            return pending;
        });
    }

    private <T> T evaluate(RedisClusterCommands<String, byte[]> commands, String script,
                           ScriptOutputType output, String[] keys, byte[]... args)
            throws RedisSessionStoreException {

        try {
            return commands.eval(script, output, keys, args);
        } catch (RedisException e) {
            throw new RedisSessionStoreException("Error while executing a Redis script after reloading it.", e);
        }
    }

    /**
     * Awaits a batch against one deadline for the whole batch, so its worst case is the command timeout
     * rather than the command timeout per reply. A command the server rejects is recorded against its
     * position and the batch is read to its end, since the commands behind it have already run.
     */
    private <T> BatchResult<T> await(List<RedisFuture<T>> pending) throws RedisSessionStoreException {

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(commandTimeout);
        List<T> replies = new ArrayList<>(pending.size());
        Map<Integer, String> failures = new LinkedHashMap<>();
        for (int index = 0; index < pending.size(); index++) {
            RedisFuture<T> future = pending.get(index);
            long remaining = deadline - System.nanoTime();
            try {
                if (remaining <= 0 || !future.await(remaining, TimeUnit.NANOSECONDS)) {
                    throw new RedisSessionStoreException("A Redis command batch of " + pending.size()
                            + " commands did not complete within " + commandTimeout + "ms; " + index
                            + " of them had replied.");
                }
                String error = future.getError();
                if (error != null) {
                    failures.put(index, error);
                    replies.add(null);
                } else {
                    // The command has replied, so this does not block.
                    replies.add(future.get());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RedisSessionStoreException("Interrupted while awaiting a Redis command batch.", e);
            } catch (ExecutionException e) {
                // A failure the server did not reply an error for, such as a connection lost mid batch.
                failures.put(index, String.valueOf(e.getCause()));
                replies.add(null);
            }
        }
        return new BatchResult<>(replies, failures);
    }

    private static boolean isScriptNotCached(RedisCommandExecutionException e) {

        return e.getMessage() != null && e.getMessage().toUpperCase(Locale.ENGLISH).contains(NO_SCRIPT_ERROR);
    }

    private String getDigest(String script) {

        return scriptDigests.computeIfAbsent(script, RedisTemplate::computeDigest);
    }

    /**
     * Redis addresses a cached script by the SHA-1 of its exact body, in lowercase hex. The algorithm is fixed by
     * the protocol, and the digest is a cache key rather than a security digest.
     *
     * @param script Script body.
     * @return the SHA-1 digest of the script, in lowercase hex.
     */
    private static String computeDigest(String script) {

        try {
            MessageDigest digest = MessageDigest.getInstance(SHA_1);
            byte[] hash = digest.digest(script.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(hash.length * 2);
            for (byte value : hash) {
                builder.append(Character.forDigit((value >> 4) & 0xF, 16));
                builder.append(Character.forDigit(value & 0xF, 16));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(SHA_1 + " is not available, so Redis scripts cannot be "
                    + "addressed by digest.", e);
        }
    }

    /**
     * The replies of a batch together with its failures, so a caller can tell which of the commands the
     * server rejected.
     *
     * @param <T> Reply type of the commands of the batch.
     */
    public static class BatchResult<T> {

        private final List<T> replies;
        private final Map<Integer, String> failures;

        BatchResult(List<T> replies, Map<Integer, String> failures) {

            this.replies = replies;
            this.failures = failures;
        }

        /**
         * The replies, in the order the commands were issued, holding {@code null} where a command failed.
         *
         * @return the replies of the batch.
         */
        public List<T> getReplies() {

            return Collections.unmodifiableList(replies);
        }

        /**
         * Whether any command of the batch failed.
         *
         * @return true when at least one command failed.
         */
        public boolean hasFailures() {

            return !failures.isEmpty();
        }

        /**
         * The failure of the command at the given position.
         *
         * @param index Position of the command in the batch.
         * @return the error the command failed with, or null when it succeeded.
         */
        public String getFailure(int index) {

            return failures.get(index);
        }

        /**
         * The failures of the batch, keyed by the position of the command in it.
         *
         * @return the failures, which is empty when every command succeeded.
         */
        public Map<Integer, String> getFailures() {

            return Collections.unmodifiableMap(failures);
        }
    }

    /**
     * One invocation of a script in a batch.
     */
    public static class ScriptCall {

        private static final byte[][] NO_ARGS = new byte[0][];

        private final String key;

        // Read by the enclosing template, so the array is never handed out of the class.
        private final byte[][] args;

        /**
         * Creates an invocation of a script that takes no arguments.
         *
         * @param key Single key the script operates on.
         */
        public ScriptCall(String key) {

            this.key = key;
            this.args = NO_ARGS;
        }

        /**
         * Creates an invocation of a script that takes one argument. Kept apart from the invocation taking
         * several, so that a single argument is never read as the whole argument list.
         *
         * @param key Single key the script operates on.
         * @param arg Single script argument.
         */
        public ScriptCall(String key, byte[] arg) {

            this.key = key;
            this.args = new byte[][]{arg};
        }

        /**
         * The key the script operates on.
         *
         * @return the key of this invocation.
         */
        public String getKey() {

            return key;
        }
    }

    /**
     * Commands executed against the synchronous command interface.
     *
     * @param <T> Result type.
     */
    @FunctionalInterface
    public interface RedisCallable<T> {

        /**
         * @param commands Synchronous command interface.
         * @return the result of the commands.
         * @throws RedisSessionStoreException if the commands fail.
         */
        T execute(RedisClusterCommands<String, byte[]> commands) throws RedisSessionStoreException;
    }

    /**
     * Commands issued to the asynchronous command interface without awaiting each of them.
     *
     * @param <T> Reply type of the issued commands.
     */
    @FunctionalInterface
    public interface RedisBatchCallable<T> {

        /**
         * @param commands Asynchronous command interface.
         * @return the issued commands, to be awaited by the template.
         * @throws RedisSessionStoreException if the commands cannot be issued.
         */
        List<RedisFuture<T>> issue(RedisClusterAsyncCommands<String, byte[]> commands)
                throws RedisSessionStoreException;
    }
}
