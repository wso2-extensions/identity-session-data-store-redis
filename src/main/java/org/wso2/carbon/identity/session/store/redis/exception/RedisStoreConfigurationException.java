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
 * Thrown when the configuration of the Redis session store is missing or unusable, which is a
 * deployment error rather than a runtime failure: the store was selected, so there is no other store
 * to fall back to, and a server that carried on would silently persist no session at all.
 *
 * <p>Unchecked, because it is thrown only while the configuration is read at activation, and its one
 * caller is the component, which lets it out so that activation fails.
 */
public class RedisStoreConfigurationException extends RuntimeException {

    private static final long serialVersionUID = 6743652282L;

    /**
     * Creates an exception with the given message.
     *
     * @param message Message describing what is missing or unusable.
     */
    public RedisStoreConfigurationException(String message) {

        super(message);
    }

    /**
     * Creates an exception with the given message and cause.
     *
     * @param message Message describing what is missing or unusable.
     * @param cause   Cause of the failure.
     */
    public RedisStoreConfigurationException(String message, Throwable cause) {

        super(message, cause);
    }
}
