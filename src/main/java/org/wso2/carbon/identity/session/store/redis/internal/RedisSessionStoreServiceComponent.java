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

package org.wso2.carbon.identity.session.store.redis.internal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.carbon.identity.application.authentication.framework.dao.UserSessionDAO;
import org.wso2.carbon.identity.application.authentication.framework.store.JavaSessionSerializer;
import org.wso2.carbon.identity.application.authentication.framework.store.SessionDataStore;
import org.wso2.carbon.identity.application.authentication.framework.store.SessionSerializer;
import org.wso2.carbon.identity.core.util.IdentityCoreInitializedEvent;
import org.wso2.carbon.identity.session.store.redis.config.RedisStoreConfig;
import org.wso2.carbon.identity.session.store.redis.dao.RedisSessionContext;
import org.wso2.carbon.identity.session.store.redis.dao.RedisSessionDataStore;
import org.wso2.carbon.identity.session.store.redis.dao.RedisUserSessionDAOImpl;

/**
 * Registers the two services of the Redis session store, the {@link SessionDataStore} and the
 * {@link UserSessionDAO}. Both are selected by the same {@code SessionStorage.Type} property, so they
 * are registered and removed together.
 *
 * <p>Both are registered whenever this component activates; it is the framework that picks a store by
 * name, so a server left on the relational one never calls these. The
 * {@link IdentityCoreInitializedEvent} reference is an ordering guard, since configuration is read here
 * at activation. Configuration that is missing or unusable is logged and leaves the component activated
 * with neither service registered, rather than registering a store that could not connect.
 */
@Component(
        name = "org.wso2.carbon.identity.session.store.redis",
        immediate = true
)
public class RedisSessionStoreServiceComponent {

    private static final Log LOG = LogFactory.getLog(RedisSessionStoreServiceComponent.class);

    @Activate
    protected void activate(ComponentContext context) {

        try {
            RedisStoreConfig config = RedisStoreConfig.load();

            RedisSessionContext sessionContext = new RedisSessionContext(config);

            RedisSessionDataStore sessionDataStore = new RedisSessionDataStore(sessionContext);
            context.getBundleContext().registerService(SessionDataStore.class.getName(), sessionDataStore, null);

            RedisUserSessionDAOImpl userSessionDAO = new RedisUserSessionDAOImpl(sessionContext);
            context.getBundleContext().registerService(UserSessionDAO.class.getName(), userSessionDAO, null);

            LOG.info("Redis session store is activated.");
        } catch (RuntimeException e) {
            LOG.error("Error while activating the Redis session store.", e);
        }
    }

    /**
     * Ordering guard only. The event itself carries nothing.
     */
    @Reference(
            name = "identity.core.init.event.service",
            service = IdentityCoreInitializedEvent.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetIdentityCoreInitializedEventService"
    )
    protected void setIdentityCoreInitializedEventService(IdentityCoreInitializedEvent event) {

        /* Ordering guard; nothing to store. */
    }

    protected void unsetIdentityCoreInitializedEventService(IdentityCoreInitializedEvent event) {

        /* Ordering guard; nothing to release. */
    }

    /**
     * Tracks the serializer a deployment has registered.
     */
    @Reference(
            name = "session.serializer",
            // Optional, since nothing registers a serializer in a default deployment and the store
            // falls back to Java serialization. A mandatory reference would leave the component
            // unsatisfied and register no store at all.
            service = SessionSerializer.class,
            cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetSessionSerializer"
    )
    protected void setSessionSerializer(SessionSerializer sessionSerializer) {

        RedisSessionStoreDataHolder.setSessionSerializer(sessionSerializer);
        if (LOG.isDebugEnabled()) {
            LOG.debug("A session serializer was registered, and the Redis session store will use it: "
                    + sessionSerializer.getClass().getName());
        }
    }

    protected void unsetSessionSerializer(SessionSerializer sessionSerializer) {

        // Restored rather than cleared, so that the serializer read per operation is never null.
        RedisSessionStoreDataHolder.setSessionSerializer(new JavaSessionSerializer());
    }
}
