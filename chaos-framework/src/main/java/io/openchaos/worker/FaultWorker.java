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

package io.openchaos.worker;

import io.openchaos.fault.Fault;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

public class FaultWorker extends Worker {

    private Fault fault;

    private int interval;
    private boolean fault_once;

    public FaultWorker(Logger log, Fault fault, int interval, boolean fault_once) {
        super("Fault worker", log, fault_once);
        this.fault = fault;
        this.interval = interval;
        this.fault_once = fault_once;
    }

    @Override
    public void loop() {
        try {
            if(fault_once){
                await(TimeUnit.SECONDS.toMillis(15));
            } else {
                await(TimeUnit.SECONDS.toMillis(interval));
            }
            fault.invoke();
            await(TimeUnit.SECONDS.toMillis(interval));
            fault.recover();
        } catch (InterruptedException e) {
            log.info("Fault loop interrupted");
        }
    }

    @Override
    public void breakLoop() {
        super.breakLoop();
        interrupt();
    }

}
