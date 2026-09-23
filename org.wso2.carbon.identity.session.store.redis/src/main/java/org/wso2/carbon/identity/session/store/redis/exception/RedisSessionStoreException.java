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

package org.wso2.carbon.identity.session.store.redis.exception;

/**
 * Thrown when a Redis operation of the session store fails. Checked, so that the store boundary is
 * required to handle it rather than letting it reach a framework caller.
 */
public class RedisSessionStoreException extends Exception {

    private static final long serialVersionUID = 6743652281L;

    /**
     * Creates an exception with the given message.
     *
     * @param message Message describing the failure.
     */
    public RedisSessionStoreException(String message) {

        super(message);
    }

    /**
     * Creates an exception with the given message and cause.
     *
     * @param message Message describing the failure.
     * @param cause   Cause of the failure.
     */
    public RedisSessionStoreException(String message, Throwable cause) {

        super(message, cause);
    }
}
