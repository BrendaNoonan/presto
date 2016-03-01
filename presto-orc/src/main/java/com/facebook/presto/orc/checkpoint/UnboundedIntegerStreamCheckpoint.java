/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc.checkpoint;

import com.facebook.presto.orc.checkpoint.Checkpoints.ColumnPositionsList;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.google.common.base.MoreObjects;

import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.inputStreamCheckpointToString;

public final class UnboundedIntegerStreamCheckpoint
        implements StreamCheckpoint
{
    private final long inputStreamCheckpoint;

    public UnboundedIntegerStreamCheckpoint(CompressionKind compressionKind, ColumnPositionsList positionsList)
    {
        inputStreamCheckpoint = createInputStreamCheckpoint(compressionKind, positionsList);
    }

    public long getInputStreamCheckpoint()
    {
        return inputStreamCheckpoint;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("inputStreamCheckpoint", inputStreamCheckpointToString(inputStreamCheckpoint))
                .toString();
    }
}