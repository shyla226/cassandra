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

/**
 * Exception that can be used in {@link VerbHandler} implementations for two-way verbs if for some reason no response
 * should be sent back (and no error logging should be performed either).
 * <p>
 * This should be used only exceptionally as it's usually preferred to answer back with a {@link FailureResponse}. In
 * rare and justified cases however, we may want the sender to timeout rather than having to handle the failure for some
 * reason, and this allows that behavior.
 */
public class DroppingResponseException extends RuntimeException
{
}
