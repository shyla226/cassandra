/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.net;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

interface MessageExecutor
{
    <P, Q, R extends Request<P, Q>> void execute(R request, Runnable runnable);
    <Q, R extends Response<Q>> void execute(R response, Runnable runnable);

    static abstract class AbstractExecutor implements MessageExecutor
    {
        protected final TracingAwareExecutor requestExecutor;
        protected final TracingAwareExecutor responseExecutor;

        protected AbstractExecutor(TracingAwareExecutor requestExecutor, TracingAwareExecutor responseExecutor)
        {
            this.requestExecutor = requestExecutor;
            this.responseExecutor = responseExecutor;
        }
    }

    static class Local extends AbstractExecutor
    {
        static final Local DIRECT = new Local(null, null);

        Local(TracingAwareExecutor requestExecutor, TracingAwareExecutor responseExecutor)
        {
            super(requestExecutor, responseExecutor);
        }

        public <P, Q, R extends Request<P, Q>> void execute(R request, Runnable runnable)
        {
            assert request.isLocal();
            if (requestExecutor == null)
                runnable.run();
            else
                requestExecutor.execute(runnable, ExecutorLocals.create());
        }

        public <Q, R extends Response<Q>> void execute(R response, Runnable runnable)
        {
            assert response.isLocal();
            if (responseExecutor == null)
                runnable.run();
            else
                responseExecutor.execute(runnable, ExecutorLocals.create());
        }

        Local withResponseExecutor(TracingAwareExecutor newResponseExecutor)
        {
            return new Local(requestExecutor, newResponseExecutor);
        }
    }

    static class Remote extends AbstractExecutor
    {
        Remote(TracingAwareExecutor requestExecutor, TracingAwareExecutor responseExecutor)
        {
            super(requestExecutor, responseExecutor);
            // Until we have a Netty-based MS, we shouldn't execute remote queries directly as that would execute them
            // on a connection thread which would be bad.
            assert requestExecutor != null && responseExecutor != null;
        }

        private ExecutorLocals locals(Message<?> m)
        {
            TraceState state = Tracing.instance.initializeFromMessage(m);
            if (state != null)
                state.trace("{} message received from {}", m.verb(), m.from());

            return ExecutorLocals.create(state, ClientWarn.instance.getForMessage(m.id()));
        }

        public <P, Q, R extends Request<P, Q>> void execute(R request, Runnable runnable)
        {
            assert !request.isLocal();
            requestExecutor.execute(runnable, locals(request));
        }

        public <Q, R extends Response<Q>> void execute(R response, Runnable runnable)
        {
            assert !response.isLocal();
            responseExecutor.execute(runnable, locals(response));
        }

        Remote withResponseExecutor(TracingAwareExecutor newResponseExecutor)
        {
            return new Remote(requestExecutor, newResponseExecutor);
        }
    }
}
