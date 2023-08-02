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

package io.openchaos.fault;

import io.openchaos.ChaosControl;
import io.openchaos.recorder.FaultLogEntry;
import io.openchaos.recorder.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The fault which do nothing
 */
public class CustomFault implements Fault {
    private static final Logger log = LoggerFactory.getLogger(ChaosControl.class);
    private Recorder recorder;
    private final String mode = "CustomFault";

    public CustomFault(Recorder recorder) {
        this.recorder = recorder;
    }

    @Override
    public void invoke() {
        log.info("Invoke {} fault....", mode);
        recorder.recordFault(new FaultLogEntry(mode, "start", System.currentTimeMillis(), mode));

    }

    @Override
    public void recover() {
        log.info("Recover {} fault....", mode);
        recorder.recordFault(new FaultLogEntry(mode, "end", System.currentTimeMillis(), mode));
    }
}
