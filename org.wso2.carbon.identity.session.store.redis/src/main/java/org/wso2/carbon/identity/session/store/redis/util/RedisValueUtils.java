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

import org.apache.commons.lang.StringUtils;
import org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Converts between the values of a Redis reply and the types the store uses.
 */
public class RedisValueUtils {

    private RedisValueUtils() {

    }

    /**
     * Converts a value to the bytes Redis holds.
     *
     * @param value String to convert.
     * @return the bytes of the given value.
     */
    public static byte[] toBytes(String value) {

        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Converts a value of a Redis reply to a string.
     *
     * @param value Value from a Redis reply.
     * @return the value as a string, or {@code null} when it is absent.
     */
    public static String toString(Object value) {

        if (value == null) {
            return null;
        }
        return value instanceof byte[] ? new String((byte[]) value, StandardCharsets.UTF_8) : String.valueOf(value);
    }

    /**
     * Converts a value of a Redis reply to a long.
     *
     * @param value Value from a Redis reply.
     * @return the value as a long, or zero when it is absent or not a number.
     */
    public static long toLong(Object value) {

        try {
            String text = toString(value);
            return StringUtils.isBlank(text) ? 0 : Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Converts a value to an integer.
     *
     * @param value Value to convert.
     * @return the value as an integer, or {@code null} when it is absent or not a number.
     */
    public static Integer toInteger(String value) {

        if (StringUtils.isBlank(value)) {
            return null;
        }
        try {
            return Integer.valueOf(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Reads a nested list of a Redis reply, which is how a script returns several values of one kind.
     *
     * @param value Element of a Redis reply.
     * @return the members as strings, and an empty list when the element is absent or not a list.
     */
    public static List<String> toMembers(Object value) {

        if (!(value instanceof List)) {
            return Collections.emptyList();
        }
        List<String> members = new ArrayList<>(((List<?>) value).size());
        for (Object member : (List<?>) value) {
            String text = toString(member);
            if (StringUtils.isNotEmpty(text)) {
                members.add(text);
            }
        }
        return members;
    }

    /**
     * Reads the user identifier a session holds.
     *
     * @param value Value of the user field of a session.
     * @return the user identifier, or {@code null} when the session holds none.
     */
    public static String toUserId(Object value) {

        String userId = toString(value);
        return StringUtils.isEmpty(userId) ? null : userId;
    }

    /**
     * Returns the remaining expiry of a record in milliseconds.
     *
     * @param expiryNano Absolute expiry in the units of {@code FrameworkUtils.getCurrentStandardNano()}.
     * @return the remaining expiry in milliseconds.
     */
    public static long getExpiryMillis(long expiryNano) {

        long expiry = TimeUnit.NANOSECONDS.toMillis(expiryNano - FrameworkUtils.getCurrentStandardNano());
        return expiry <= 0 ? expiry : Math.max(expiry, RedisConstants.MIN_EXPIRY_MILLIS);
    }
}
