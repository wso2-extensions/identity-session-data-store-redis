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

import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandType;
import org.junit.jupiter.api.Test;
import org.wso2.carbon.identity.session.store.redis.exception.RedisSessionStoreException;

import java.lang.reflect.Proxy;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that every command path reports a timeout to the connection manager, which reroutes the commands that
 * follow in cluster mode, and that other failures are not reported as one.
 */
public class RedisTemplateTest {

    private static final long COMMAND_TIMEOUT = 50L;
    private static final RedisCodec<String, byte[]> CODEC =
            RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);

    /**
     * A connection manager whose commands all fail with the given exception, and which counts the timeouts
     * reported to it.
     */
    private static class TimeoutCountingConnectionManager extends RedisConnectionManager {

        private final RedisException failure;
        private int timeouts;

        TimeoutCountingConnectionManager(RedisException failure) {

            this.failure = failure;
        }

        @Override
        @SuppressWarnings("unchecked")
        public RedisClusterCommands<String, byte[]> getCommands() {

            return (RedisClusterCommands<String, byte[]>) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{RedisClusterCommands.class}, (proxy, method, args) -> {
                        throw failure;
                    });
        }

        @Override
        public RedisClusterAsyncCommands<String, byte[]> getAsyncCommands() {

            // The batches of these tests return their futures without issuing anything.
            return null;
        }

        @Override
        public void refreshClusterConnection() {

            timeouts++;
        }

        @Override
        public void close() {

        }
    }

    @Test
    void testCommandTimeoutIsReported() {

        TimeoutCountingConnectionManager manager =
                new TimeoutCountingConnectionManager(new RedisCommandTimeoutException());
        RedisTemplate template = new RedisTemplate(manager, COMMAND_TIMEOUT);

        assertThrows(RedisSessionStoreException.class,
                () -> template.execute(commands -> commands.get("key")));
        assertEquals(1, manager.timeouts);
    }

    @Test
    void testScriptTimeoutIsReported() {

        TimeoutCountingConnectionManager manager =
                new TimeoutCountingConnectionManager(new RedisCommandTimeoutException());
        RedisTemplate template = new RedisTemplate(manager, COMMAND_TIMEOUT);

        assertThrows(RedisSessionStoreException.class,
                () -> template.executeScript("return 1", ScriptOutputType.INTEGER, "key"));
        assertEquals(1, manager.timeouts);
    }

    @Test
    void testOtherFailuresAreNotReportedAsTimeouts() {

        // A refused or lost connection is recovered by reconnecting, not by rerouting to another node.
        TimeoutCountingConnectionManager manager =
                new TimeoutCountingConnectionManager(new RedisConnectionException("Connection refused"));
        RedisTemplate template = new RedisTemplate(manager, COMMAND_TIMEOUT);

        assertThrows(RedisSessionStoreException.class,
                () -> template.execute(commands -> commands.get("key")));
        assertThrows(RedisSessionStoreException.class,
                () -> template.executeScript("return 1", ScriptOutputType.INTEGER, "key"));
        assertEquals(0, manager.timeouts);
    }

    @Test
    void testBatchPastItsDeadlineIsReported() {

        TimeoutCountingConnectionManager manager = new TimeoutCountingConnectionManager(null);
        RedisTemplate template = new RedisTemplate(manager, COMMAND_TIMEOUT);

        // A reply that never arrives, as from a node that stopped responding without closing its socket.
        RedisSessionStoreException exception = assertThrows(RedisSessionStoreException.class,
                () -> template.collectBatch(commands -> List.of(pendingCommand())));
        assertTrue(exception.getMessage().contains("did not complete"));
        assertEquals(1, manager.timeouts);
    }

    @Test
    void testTimedOutCommandInABatchIsReported() throws Exception {

        TimeoutCountingConnectionManager manager = new TimeoutCountingConnectionManager(null);
        RedisTemplate template = new RedisTemplate(manager, COMMAND_TIMEOUT);
        AsyncCommand<String, byte[], String> timedOut = pendingCommand();
        timedOut.completeExceptionally(new RedisCommandTimeoutException());

        RedisTemplate.BatchResult<String> result = template.collectBatch(commands -> List.of(timedOut));
        assertTrue(result.hasFailures());
        assertEquals(1, manager.timeouts);
    }

    private static AsyncCommand<String, byte[], String> pendingCommand() {

        return new AsyncCommand<>(new Command<>(CommandType.GET, new StatusOutput<>(CODEC)));
    }
}
