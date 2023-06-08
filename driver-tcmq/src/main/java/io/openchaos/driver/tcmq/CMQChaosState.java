/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openchaos.driver.tcmq;

import io.openchaos.driver.queue.QueueState;
import java.util.HashSet;
import java.util.Set;

public class CMQChaosState implements QueueState {

    @Override
    public void initialize(String metaName, String metaNode) {

    }

    @Override
    public Set<String> getLeader() {
        Set<String> leaderAddrPort = new HashSet<>();

        return leaderAddrPort;
    }

    @Override
    public void close() {

    }
}
