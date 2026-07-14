# MuleGraph Phase 5 Metric Evaluation Report

## Setup
- **Seed**: 42 (Fixed deterministic seed)
- **Dataset Size**: 275 transactions injected via REST API
- **Expected Scenarios**: 45

## Results
- **Total Expected Alerts**: 45
- **Total Actual Alerts Generated**: 0
- **True Positives (TP)**: 0
- **False Positives (FP)**: 0
- **False Negatives (FN)**: 45

## Metrics
- **Precision**: 0.0000
- **Recall**: 0.0000
- **F1 Score**: 0.0000
- **False Positive Rate (FPR)**: 0.0000
- **False Negative Rate (FNR)**: 1.0000
- **Average Detection Latency**: 0 ms

## Missed Scenarios (FN)
- ExpectedAlert[ruleType=FAN_OUT, primaryAccountId=820bd70a-85b9-429d-8d0d-118813d3dd85, scenarioId=FO_0]
- ExpectedAlert[ruleType=FAN_OUT, primaryAccountId=6270903b-cf56-4ea2-a596-b478a4d9336d, scenarioId=FO_1]
- ExpectedAlert[ruleType=FAN_OUT, primaryAccountId=8af7e53e-fb86-4989-989b-62bb2a1f7036, scenarioId=FO_2]
- ExpectedAlert[ruleType=FAN_OUT, primaryAccountId=194e8714-f9b7-4a66-8f68-95dd75c1b8bb, scenarioId=FO_3]
- ExpectedAlert[ruleType=FAN_OUT, primaryAccountId=a2da2f25-a9fe-4aaf-82e9-1edaf9839415, scenarioId=FO_4]
- ExpectedAlert[ruleType=FAN_OUT, primaryAccountId=bcd4b1da-b359-490b-a930-04705c533e5b, scenarioId=FO_5]
- ExpectedAlert[ruleType=FAN_OUT, primaryAccountId=14c8a183-14a9-47f0-ac61-20c5cf3fa2b1, scenarioId=FO_6]
- ExpectedAlert[ruleType=FAN_OUT, primaryAccountId=84a96bfe-1001-49c0-804b-b5034e7a46ec, scenarioId=FO_7]
- ExpectedAlert[ruleType=FAN_OUT, primaryAccountId=103b5fc9-fa1f-49fe-8af9-379a923cb703, scenarioId=FO_8]
- ExpectedAlert[ruleType=FAN_OUT, primaryAccountId=ace56ea7-7cc3-440c-876b-4b91c9915b02, scenarioId=FO_9]
- ExpectedAlert[ruleType=FAN_IN, primaryAccountId=d3ff3118-f1b5-41f1-90be-d85d1161c6db, scenarioId=FI_0]
- ExpectedAlert[ruleType=FAN_IN, primaryAccountId=24046d29-ad20-4d27-a573-f637638911d2, scenarioId=FI_1]
- ExpectedAlert[ruleType=FAN_IN, primaryAccountId=8d4dbac9-e690-4001-8d23-dd4b594c55a6, scenarioId=FI_2]
- ExpectedAlert[ruleType=FAN_IN, primaryAccountId=2f854879-45d5-4964-ab6a-2272a921e341, scenarioId=FI_3]
- ExpectedAlert[ruleType=FAN_IN, primaryAccountId=c04a7208-a442-4f0d-a6b2-e8216d80f7fc, scenarioId=FI_4]
- ExpectedAlert[ruleType=FAN_IN, primaryAccountId=2ccb516d-4b95-42b9-809e-3759811cf387, scenarioId=FI_5]
- ExpectedAlert[ruleType=FAN_IN, primaryAccountId=c3209a99-4a47-4a0d-af2b-bddf9ebfb30d, scenarioId=FI_6]
- ExpectedAlert[ruleType=FAN_IN, primaryAccountId=bba20f49-f440-47f2-8117-cf46dcaf051e, scenarioId=FI_7]
- ExpectedAlert[ruleType=FAN_IN, primaryAccountId=1c1b2a05-c455-4acc-b0b5-9124f0b16dc4, scenarioId=FI_8]
- ExpectedAlert[ruleType=FAN_IN, primaryAccountId=8430e1e8-1616-4b11-ad78-28d4cdd9c7ec, scenarioId=FI_9]
- ExpectedAlert[ruleType=SHARED_DEVICE, primaryAccountId=d359b2ca-6fe2-3e90-9057-9299892397be, scenarioId=SD_0]
- ExpectedAlert[ruleType=SHARED_DEVICE, primaryAccountId=84427f68-87eb-348d-aac3-33d8140861a0, scenarioId=SD_1]
- ExpectedAlert[ruleType=SHARED_DEVICE, primaryAccountId=e9029ce6-3e37-3b2c-930e-a00459dccb60, scenarioId=SD_2]
- ExpectedAlert[ruleType=SHARED_DEVICE, primaryAccountId=f3fd2b4e-584c-3117-a23e-71b6ae0702c9, scenarioId=SD_3]
- ExpectedAlert[ruleType=SHARED_DEVICE, primaryAccountId=0ce0a0c4-57bb-3642-b094-8cc844e39205, scenarioId=SD_4]
- ExpectedAlert[ruleType=SHARED_DEVICE, primaryAccountId=d5ad6c85-fb70-3e56-9761-8e4ff6bf8905, scenarioId=SD_5]
- ExpectedAlert[ruleType=SHARED_DEVICE, primaryAccountId=25a6b335-fe4d-35a5-8650-61fa53a8a47b, scenarioId=SD_6]
- ExpectedAlert[ruleType=SHARED_DEVICE, primaryAccountId=9fa5851a-da1c-3587-b363-62020fc41636, scenarioId=SD_7]
- ExpectedAlert[ruleType=SHARED_DEVICE, primaryAccountId=895ad1f5-72e4-3495-9f4d-fa24ffa1dc8a, scenarioId=SD_8]
- ExpectedAlert[ruleType=SHARED_DEVICE, primaryAccountId=fbacb8fc-529a-3aef-819a-1a97cd615ad4, scenarioId=SD_9]
- ExpectedAlert[ruleType=SHARED_IP, primaryAccountId=44623682-48c3-3f60-a9e3-360f369c3214, scenarioId=SI_0]
- ExpectedAlert[ruleType=SHARED_IP, primaryAccountId=d3743b7d-1427-3040-91fc-844c07b5206d, scenarioId=SI_1]
- ExpectedAlert[ruleType=SHARED_IP, primaryAccountId=8fe58f4a-49b5-35ce-be55-f0140b2a2a59, scenarioId=SI_2]
- ExpectedAlert[ruleType=SHARED_IP, primaryAccountId=30e6add7-bf69-3bc1-b65b-6b4ade654275, scenarioId=SI_3]
- ExpectedAlert[ruleType=SHARED_IP, primaryAccountId=3af45cb3-8f12-3f6a-8378-9a0651c32734, scenarioId=SI_4]
- ExpectedAlert[ruleType=SHARED_IP, primaryAccountId=dfd23ed6-149a-315c-8cd9-b7e0d0c52cb5, scenarioId=SI_5]
- ExpectedAlert[ruleType=SHARED_IP, primaryAccountId=5c99d9f5-c843-3ebf-af90-80f07a95f13c, scenarioId=SI_6]
- ExpectedAlert[ruleType=SHARED_IP, primaryAccountId=4080fbb3-112a-35ec-9b4f-343cdbc97331, scenarioId=SI_7]
- ExpectedAlert[ruleType=SHARED_IP, primaryAccountId=96fb8fef-66da-3af3-95d6-e1f144b32aa0, scenarioId=SI_8]
- ExpectedAlert[ruleType=SHARED_IP, primaryAccountId=dd20face-d8f9-3a9d-abd4-006e2eb6a01c, scenarioId=SI_9]
- ExpectedAlert[ruleType=CIRCULAR_FLOW, primaryAccountId=6835c8e3-c5e7-402a-aec1-1f6c4a7b3878, scenarioId=CF_0]
- ExpectedAlert[ruleType=CIRCULAR_FLOW, primaryAccountId=617fbc0e-f962-43fc-8409-3ec890eecf1d, scenarioId=CF_1]
- ExpectedAlert[ruleType=CIRCULAR_FLOW, primaryAccountId=8581cb12-e403-4e9e-981b-1ff2eedb47cb, scenarioId=CF_2]
- ExpectedAlert[ruleType=CIRCULAR_FLOW, primaryAccountId=9911a86f-842d-4927-a9dd-6aef88580f80, scenarioId=CF_3]
- ExpectedAlert[ruleType=CIRCULAR_FLOW, primaryAccountId=acaf6ec2-eb48-49d8-a3ae-287ba7d7df89, scenarioId=CF_4]

