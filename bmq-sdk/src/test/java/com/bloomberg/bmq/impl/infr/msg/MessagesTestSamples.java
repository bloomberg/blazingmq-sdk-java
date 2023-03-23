/*
 * Copyright 2022 Bloomberg Finance L.P.
 *
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
package com.bloomberg.bmq.impl.infr.msg;

public class MessagesTestSamples {
    public static class SampleFileMetadata {
        private final String filePath;
        private int length;

        public SampleFileMetadata(String fileName, int length) {
            this.filePath = fileName;
            this.length = length;
        }

        public int length() {
            return length;
        }

        public String filePath() {
            return filePath;
        }

        public SampleFileMetadata setLength(int val) {
            length = val;
            return this;
        }
    }

    public static final SampleFileMetadata BMQ_IO_DUMP_BIN =
            new SampleFileMetadata("/data/bmq_io_dump_1551267643131.bin", 16520);
    public static final SampleFileMetadata BMQ_IO_DUMP_IDX =
            new SampleFileMetadata("/data/bmq_io_dump_1551267643131.idx", 2178);

    public static final SampleFileMetadata STATUS_MSG =
            new SampleFileMetadata(
                    "/data/msg_control_status_53121b03-f45d-46b2-95d0-f2df8a1a2cb2.bin", 36);
    public static final SampleFileMetadata STATUS_MSG_JSON =
            new SampleFileMetadata(
                    "/data/msg_control_status_53121b03-f45d-46b2-95d0-f2df8a1a2cb2.bin", 72);
    public static final SampleFileMetadata STATUS_MSG_ID_JSON =
            new SampleFileMetadata("/data/msg_control_status_rid_json_05092018.bin", 80);
    public static final SampleFileMetadata STATUS_MSG_27032018 =
            new SampleFileMetadata("/data/msg_control_status_27032018.bin", 128);
    public static final SampleFileMetadata STATUS_MSG_FRAGMENT =
            new SampleFileMetadata("/data/msg_control_status_with_fragment_bit_22072019.bin", 128);
    public static final SampleFileMetadata OPEN_QUEUE =
            new SampleFileMetadata("/data/msg_control_open_queue_06042018.bin", 80);
    public static final SampleFileMetadata OPEN_QUEUE_JSON =
            new SampleFileMetadata(
                    "/data/msg_control_open_queue_5e110b7c-e332-4f5b-8313-c419ad0fbf48.bin", 156);
    public static final SampleFileMetadata OPEN_QUEUE_ID_JSON =
            new SampleFileMetadata("/data/msg_control_open_queue_json_with_id_05092018.bin", 164);
    public static final SampleFileMetadata OPEN_QUEUE_RESPONSE_JSON =
            new SampleFileMetadata(
                    "/data/msg_control_open_queue_response_9950845d-d1dc-418f-811a-1b620c73e589.bin",
                    220);
    public static final SampleFileMetadata OPEN_QUEUE_RESPONSE_ID_JSON =
            new SampleFileMetadata(
                    "/data/msg_control_open_queue_response_id_json_06092018.bin", 228);
    public static final SampleFileMetadata BROKER_RESPONSE =
            new SampleFileMetadata("/data/broker_response_20032018.bin", 104);
    public static final SampleFileMetadata BROKER_RESPONSE_JSON =
            new SampleFileMetadata(
                    "/data/msg_control_nego_client_broker_response_623ff064-7d51-4bd8-988d-826ccfd458a8.bin",
                    396);
    public static final SampleFileMetadata CLIENT_IDENTITY =
            new SampleFileMetadata("/data/msg_control_nego_client_04042018.bin", 68);
    public static final SampleFileMetadata CLIENT_IDENTITY_JSON =
            new SampleFileMetadata(
                    "/data/msg_control_nego_client_a326573a-2c1e-42a2-afdc-da184456c118.bin", 248);
    public static final SampleFileMetadata ACK_MSG =
            new SampleFileMetadata("/data/msg_ack_20072018.bin", 252);
    public static final SampleFileMetadata CONFIRM_MSG =
            new SampleFileMetadata("/data/msg_confirm_15082018.bin", 252);
    public static final SampleFileMetadata PUSH_MULTI_MSG =
            new SampleFileMetadata("/data/msg_push_multi.bin", 256);
    public static final SampleFileMetadata PUSH_MSG_ZLIB =
            new SampleFileMetadata("/data/msg_push_zlib.bin", 64);
    public static final SampleFileMetadata PUSH_WITH_SUBQUEUE_IDS_MSG =
            new SampleFileMetadata("/data/msg_push_subqueueids.bin", 128);
    public static final SampleFileMetadata PUT_MSG_ZLIB =
            new SampleFileMetadata("/data/msg_put_zlib_27042018.bin", 132);
    public static final SampleFileMetadata PUT_MULTI_MSG =
            new SampleFileMetadata("/data/msg_put_multi.bin", 264);
    public static final SampleFileMetadata MSG_PROPS_OLD =
            new SampleFileMetadata("/data/msg_props_old.bin", 64);
    public static final SampleFileMetadata MSG_PROPS =
            new SampleFileMetadata("/data/msg_props.bin", 64);
    public static final SampleFileMetadata MSG_PROPS_LONG_HEADERS =
            new SampleFileMetadata("/data/msg_props_long_headers.bin", 72);
    public static final SampleFileMetadata STATS_EVENTQUEUE_EMPTY =
            new SampleFileMetadata("/data/stat_eventqueue_stats_empty.txt", 321);
    public static final SampleFileMetadata STATS_EVENTQUEUE_EMPTY_FINAL =
            new SampleFileMetadata("/data/stat_eventqueue_stats_empty_final.txt", 99);
    public static final SampleFileMetadata STATS_EVENTQUEUE_SAMPLE =
            new SampleFileMetadata("/data/stat_eventqueue_stats_sample.txt", 381);
    public static final SampleFileMetadata STATS_EVENTQUEUE_SAMPLE_FINAL =
            new SampleFileMetadata("/data/stat_eventqueue_stats_sample_final.txt", 99);
    public static final SampleFileMetadata STATS_EVENTS_EMPTY =
            new SampleFileMetadata("/data/stat_events_stats_empty.txt", 496);
    public static final SampleFileMetadata STATS_EVENTS_EMPTY_FINAL =
            new SampleFileMetadata("/data/stat_events_stats_empty_final.txt", 261);
    public static final SampleFileMetadata STATS_EVENTS_SAMPLE =
            new SampleFileMetadata("/data/stat_events_stats_sample.txt", 576);
    public static final SampleFileMetadata STATS_EVENTS_SAMPLE_FINAL =
            new SampleFileMetadata("/data/stat_events_stats_sample_final.txt", 296);
    public static final SampleFileMetadata STATS_QUEUES_EMPTY =
            new SampleFileMetadata("/data/stat_queues_stats_empty.txt", 505);
    public static final SampleFileMetadata STATS_QUEUES_EMPTY_FINAL =
            new SampleFileMetadata("/data/stat_queues_stats_empty_final.txt", 256);
    public static final SampleFileMetadata STATS_QUEUES_SAMPLE =
            new SampleFileMetadata("/data/stat_queues_stats_sample.txt", 1085);
    public static final SampleFileMetadata STATS_QUEUES_SAMPLE_FINAL =
            new SampleFileMetadata("/data/stat_queues_stats_sample_final.txt", 653);
}
