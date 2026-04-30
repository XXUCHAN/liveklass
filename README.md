# liveklass

OpenSearch 기반 클릭스트림 이벤트 파이프라인 과제입니다.  
온라인 강의 탐색 서비스를 가정하고 이벤트 생성, 수집, 저장, 데이터 품질 점검, 집계 분석까지 구현했습니다.

## 1. 구현 범위

- Step 1. 이벤트 생성기 작성
- Step 2. 로그 저장
- Step 3. 데이터 집계 분석
- Step 4. Docker Compose 기반 실행
- Step 5. 결과 시각화
- 저장 후 데이터 품질 점검
- OpenSearch Dashboards 연동

## 2. 아키텍처

```text
Clickstream Generator
  ↓ HTTP POST /events
Ingestion API (FastAPI)
  ↓ Validation
  ↓ In-memory Queue
  ↓ Bulk Insert
OpenSearch
  ├── clickstream-events
  ├── dead-letter-events
  ├── data-quality-results
  └── quality-checkpoints
        ↓
Analytics Batch
  └── output/aggregations/*.json
        ↓
Visualization Batch
  └── output/charts/*.png
        ↓
OpenSearch Dashboards
```

## 3. 실행 방법

### 필요한 도구

- Docker
- Docker Compose

### 설치 예시

macOS(Homebrew) 기준 예시는 아래와 같다.

```bash
brew install --cask docker
```

설치 후 Docker Desktop을 실행한다.

### 실행 순서

1. 레포지토리 클론

```bash
git clone <repository-url>
cd liveklass
```

2. 전체 스택 실행

```bash
docker compose up
```

코드 변경 후 이미지를 다시 반영해야 할 때는 아래 명령을 사용한다.

```bash
docker compose up --build
```

3. 동작 확인

```bash
curl http://localhost:8000/health
curl http://localhost:9200/clickstream-events/_count
```

접속 주소:

- Ingestion API: `http://localhost:8000/health`
- OpenSearch: `http://localhost:9200`
- OpenSearch Dashboards: `http://localhost:5601`

산출물:

- 집계 JSON: `output/aggregations/`
- 차트 PNG: `output/charts/`

## 4. 이벤트 설계

이벤트 생성기는 온라인 강의/코스 탐색 서비스를 가정했다.  
사용자가 페이지에 진입하고, 강의 목록을 노출받고, 일부를 클릭하고, 경우에 따라 구매하거나 에러를 경험하는 흐름을 세션 단위로 생성한다.

### 이벤트 타입

- `page_view`
- `impression`
- `click`
- `purchase`
- `error`

위 이벤트들은 온라인 강의 탐색 서비스에서 실제로 자주 발생하는 흐름을 기준으로 선택했다.

- `page_view`: 사용자가 강의 목록 페이지에 진입했는지 확인하기 위한 기본 이벤트
- `impression`: 어떤 강의가 몇 번째 위치에 노출되었는지 기록하기 위한 이벤트
- `click`: 사용자가 실제로 관심을 보인 강의를 기록하기 위한 이벤트
- `purchase`: 클릭 이후 구매 전환이 일어났는지 보기 위한 이벤트
- `error`: 서비스 운영 중 발생할 수 있는 실패 상황을 추적하기 위한 이벤트

특히 `impression`과 `click`을 분리한 이유는 CTR 같은 클릭 기반 분석을 하기 위해서다.  
단순히 랜덤 이벤트를 생성하는 대신, `page_view → impression → click → purchase/error` 흐름이 보이도록 설계해 이후 집계와 시각화에서 의미 있는 패턴을 확인할 수 있게 했다.

### 클릭 생성 방식

클릭은 완전 랜덤이 아니라 아래 3가지 bias를 반영한다.

- `Position Bias`: 상위 rank일수록 클릭 확률이 높음
- `Popularity Bias`: 인기 강의일수록 클릭 확률이 높음
- `Presentation Bias`: UI 노출 방식에 따라 클릭 확률이 달라짐

### 공통 필드

- `event_id`
- `schema_version`
- `event_type`
- `user_id`
- `session_id`
- `event_time`
- `received_at`
- `device`

### 이벤트별 필드

- `page_view`: `page_url`
- `impression`: `query`, `item_id`, `rank`, `popularity_bucket`, `presentation_type`, `position_bias`
- `click`: `query`, `item_id`, `rank`, `popularity_bucket`, `presentation_type`, `position_bias`, `click_prob`
- `purchase`: `item_id`, `amount`
- `error`: `error_code`, `error_message`

## 5. 저장소 설계

저장소는 OpenSearch를 선택했다.

선택 이유:

- 이벤트 로그는 반정형 데이터라 문서형 저장소와 잘 맞는다.
- 필드 기반 검색, 필터링, 집계가 쉽다.
- OpenSearch Dashboards로 저장된 데이터를 바로 탐색할 수 있다.
- 이번 과제는 로그 저장과 집계 분석이 중심이라 PostgreSQL보다 OpenSearch가 더 적합하다고 판단했다.

### 저장 흐름

- `generator`가 `POST /events`로 ingestion API에 이벤트 전송
- ingestion API가 이벤트 단건 validation 수행
- 정상 이벤트는 메모리 큐를 거쳐 OpenSearch bulk insert
- 비정상 이벤트는 `dead-letter-events`에 저장

DLQ 동작을 확인하기 위해, generator는 일정 확률로 `device` 필드가 누락된 invalid `page_view` 이벤트를 함께 생성한다.

### 인덱스 구성

- `clickstream-events`: 정상 이벤트 저장
- `dead-letter-events`: validation 실패 이벤트 저장
- `data-quality-results`: 품질 점검 결과 저장
- `quality-checkpoints`: 증분 품질 점검 체크포인트 저장

`dead-letter-events`에는 아래와 같은 필드가 저장된다.

- `event_id`
- `event_type`
- `source_service`
- `payload`
- `error_reason`
- `validation_errors`
- `failed_stage`
- `created_at`

## 6. 스키마 설명

`clickstream-events`는 공통 필드와 이벤트별 상세 필드를 나눠 저장한다.  
집계와 필터링이 많은 필드는 `keyword`, 시간 분석용 필드는 `date`, 수치 계산용 필드는 숫자 타입으로 두었다.

| Field                 | Type      |
| --------------------- | --------- |
| `event_id`            | `keyword` |
| `schema_version`      | `keyword` |
| `event_type`          | `keyword` |
| `user_id`             | `keyword` |
| `session_id`          | `keyword` |
| `device`              | `keyword` |
| `page_url`            | `keyword` |
| `query`               | `keyword` |
| `item_id`             | `keyword` |
| `rank`                | `integer` |
| `popularity_bucket`   | `keyword` |
| `presentation_type`   | `keyword` |
| `position_bias`       | `float`   |
| `click_prob`          | `float`   |
| `amount`              | `float`   |
| `error_code`          | `keyword` |
| `error_message`       | `text`    |
| `event_time`          | `date`    |
| `received_at`         | `date`    |
| `ingested_at`         | `date`    |
| `arrival_lag_seconds` | `float`   |
| `is_late_arrival`     | `boolean` |

## 7. Validation과 Data Quality Check

입력 검증과 저장 후 정합성 점검을 분리했다.

- `validation`: ingestion 시점에 이벤트 1건씩 검사
- `data quality check`: 저장된 이벤트를 다시 검사

예를 들어 `device` 필드가 없는 이벤트는 validation 단계에서 실패하고, `dead-letter-events` 인덱스로 이동한다.

품질 점검은 주기적으로 실행되며, `ingested_at`과 `quality-checkpoints`를 사용한 증분 검사 방식으로 동작한다.

현재 구현한 품질 점검 항목:

- required field null check
- invalid event type check
- duplicate `event_id` check
- `rank` range check
- `click_prob` range check
- `position_bias` range check
- same `item_id/session_id/query` 기준 `click > impression` 여부
- late-arriving event 비율 점검

## 8. 집계 분석

현재 구현한 집계 항목은 아래 5개다.

- 이벤트 타입별 발생 횟수
- 에러 이벤트 비율
- Rank별 Raw CTR / Regression-adjusted CTR
- Popularity bucket별 Raw CTR / Rank-standardized CTR / Regression-adjusted CTR
- Presentation type별 Raw CTR / Rank-standardized CTR / Regression-adjusted CTR

집계 로직은 도메인별로 분리했다.

- `analytics/metrics/aggregate.py`
- `analytics/metrics/helper.py`
- `analytics/metrics/basic`
  - `event_type_counts.py`
  - `error_event_ratio.py`
- `analytics/metrics/click`
  - `rank_ctr.py`
  - `popularity_ctr.py`
  - `presentation_ctr.py`

집계 결과는 아래 JSON 파일로 저장된다.

- `output/aggregations/event_type_counts.json`
- `output/aggregations/error_event_ratio.json`
- `output/aggregations/rank_ctr.json`
- `output/aggregations/popularity_ctr.json`
- `output/aggregations/presentation_ctr.json`

클릭 관련 집계는 단순 CTR만 보여주지 않고 세 단계로 나눠 비교한다.  
`raw_ctr`는 편향이 섞인 관측값이고, `rank_standardized_ctr`는 popularity/presentation 그룹을 rank별로 나눠 본 뒤 전체 rank 분포로 가중 평균한 1차 보정값이다.  
`regression_adjusted_ctr`는 `click ~ rank + device + presentation + popularity` 형태의 로지스틱 회귀로 클릭 확률을 추정한 뒤, 동일한 기준 분포에서 각 그룹의 보정 CTR을 계산한 2차 보정값이다.

현재 결과 해석은 아래 기준으로 본다.

- `rank_ctr`: `raw_ctr`로 position bias가 얼마나 강한지 보고, `regression_adjusted_ctr`로 다른 요인을 통제했을 때 rank 효과가 얼마나 남는지 본다.
- `popularity_ctr`: `rank_standardized_ctr`를 주 해석 지표로 사용한다. popularity 항목의 `regression_adjusted_ctr`는 추가 보정 시도 결과로 저장하지만, 현재 단순 모델에서는 과보정 가능성이 있어 보조 지표로 본다.
- `presentation_ctr`: `raw_ctr`, `rank_standardized_ctr`, `regression_adjusted_ctr`가 비슷한 방향으로 움직이는지 보고, UI 표현 효과가 안정적으로 관측되는지 확인한다.

## 9. Dashboards 확인

OpenSearch Dashboards에서 저장된 이벤트와 품질 점검 결과를 확인할 수 있다.

- `http://localhost:5601`

예시 쿼리:

```http
GET clickstream-events/_count
```

```http
GET dead-letter-events/_search
{
  "size": 10,
  "sort": [
    { "created_at": "desc" }
  ]
}
```

```http
GET data-quality-results/_search
{
  "size": 20,
  "sort": [
    { "checked_at": "desc" }
  ]
}
```

## 10. 시각화

집계 결과 JSON을 읽어 아래 PNG 파일을 생성한다.

- `analytics/visualization/visualize.py`

- `output/charts/event_type_counts.png`
- `output/charts/error_event_ratio.png`
- `output/charts/rank_ctr.png`
- `output/charts/popularity_ctr.png`
- `output/charts/presentation_ctr.png`

시각화는 단순 클릭 수가 아니라 `raw CTR`, `rank-standardized CTR`, `regression-adjusted CTR`를 비교하는 방향으로 구성했다.

## 11. 구현하면서 고민한 점

- 완전 랜덤 로그보다 실제 사용자 흐름처럼 보이는 클릭스트림을 만드는 것이 더 중요하다고 판단해 세션 기반으로 이벤트를 설계했다.
- `impression`과 `click`을 분리해야 CTR과 bias 기반 분석이 가능하다고 봤다.
- 클릭 분석은 처음에는 generator가 기록한 bias 값을 직접 쓰는 보정 방식으로 시작했지만, 그보다 관측된 로그만으로 해석 가능한 `rank-standardized CTR`과 `regression-adjusted CTR`이 더 설득력 있다고 판단해 집계 방향을 바꿨다.
- 저장 전 validation과 저장 후 quality check는 역할이 다르기 때문에 분리하는 쪽이 자연스럽다고 판단했다.
- 품질 점검은 전체 스캔보다 `ingested_at` 기반 증분 검사와 체크포인트 방식이 더 적절하다고 판단했다.
- 제출 환경을 고려해 `.env` 없이도 `docker compose up`으로 실행되도록 기본값 중심으로 구성했다.
- generator는 주기적으로 이벤트를 넣도록 두고, analytics와 visualizer는 한 번 실행 후 종료되는 배치 잡으로 분리했다. 계속 실행되는 서비스와 결과물을 만드는 배치 작업의 성격이 다르다고 판단했기 때문이다.
- 큐는 Kafka 같은 외부 메시지 시스템 대신 in-memory queue로 구현했다. 과제 범위에서는 구조를 이해하기 쉽게 유지하는 것이 더 중요하다고 판단했고, 대신 batch flush와 queue size 제한으로 기본적인 적재 흐름은 확인할 수 있게 했다.

## 12. 한계 및 확장성

- 현재는 in-memory queue를 사용하므로 프로세스 종료 시 메모리 버퍼가 유실될 수 있다.
- 이벤트 유입량이 커지는 환경에서는 이 부분을 Kafka, Kinesis 같은 durable queue로 교체하는 방향을 고려했다.
- 현재는 단일 OpenSearch 노드 기준이지만, 실제 확장 시에는 인덱스 분할, rollover, shard 조정으로 대응할 수 있다.
- generator, ingestion, quality, analytics, visualizer를 분리해 두었기 때문에 트래픽 증가 시 각 단계를 독립적으로 확장하기 쉽다.
- invalid event는 `device` 누락 한 가지 시나리오만 사용하고 있어, 더 다양한 실패 유형 검증은 추가 여지가 있다.
- `regression_adjusted_ctr`는 경량 로지스틱 회귀 기반의 보정이므로, 변수 간 복잡한 상호작용이나 더 정교한 causal correction까지는 반영하지 못한다. 현재 결과에서는 popularity 항목에서 `rank_standardized_ctr`가 더 안정적인 해석 지표로 보였다.

## 13. 선택 A. Kubernetes

`k8s/` 아래에 전체 파이프라인을 Kubernetes에서 운영한다고 가정한 manifest 파일을 추가했다.

- `k8s/namespace.yaml`
- `k8s/pipeline-configmap.yaml`
- `k8s/output-pvc.yaml`
- `k8s/opensearch-service.yaml`
- `k8s/opensearch-statefulset.yaml`
- `k8s/ingestion-service.yaml`
- `k8s/ingestion-deployment.yaml`
- `k8s/generator-deployment.yaml`
- `k8s/quality-cronjob.yaml`
- `k8s/reporting-cronjob.yaml`

선택한 리소스의 역할:

- `Namespace`: 관련 리소스를 `liveklass` 네임스페이스로 분리한다.
- `ConfigMap`: generator 실행에 꼭 필요한 설정만 환경변수로 분리한다.
- `Service`: `ingestion-api`, `opensearch`처럼 다른 Pod가 고정된 이름으로 접근해야 하는 컴포넌트의 네트워크 진입점을 제공한다.
- `StatefulSet`: OpenSearch처럼 디스크를 가지는 상태 저장 컴포넌트를 실행한다.
- `PersistentVolumeClaim`: OpenSearch 데이터와 analytics/visualizer 산출물을 저장할 볼륨을 확보한다.
- `Deployment`: ingestion API와 generator처럼 계속 실행되어야 하는 프로세스를 관리한다.
- `CronJob`: quality와 reporting처럼 주기 실행되는 배치 작업을 스케줄링한다.

선택한 이유:

- OpenSearch는 상태 저장 스토리지와 함께 운영해야 하므로 `StatefulSet + Service + PersistentVolumeClaim` 조합이 가장 자연스럽다고 판단했다.
- ingestion API와 generator는 계속 살아 있어야 하는 프로세스라 `Deployment`로 두는 편이 맞다고 봤다.
- ingestion API는 현재 in-memory queue를 사용하므로, Kubernetes manifest에서는 queue 일관성을 위해 `replicas: 1` 기준으로 두었다.
- ingestion API는 다른 Pod가 접근해야 하므로 `Service`가 필요하다.
- quality는 저장 후 정합성 점검을 반복 실행해야 하므로 `CronJob`이 적합하다.
- analytics와 visualizer는 둘 다 배치성 작업이고 출력 경로를 공유하므로, `reporting CronJob` 하나에서 순서대로 실행하도록 단순화했다.
- reporting이 만든 JSON과 PNG를 남기기 위해 output 경로는 별도 `PersistentVolumeClaim`으로 분리했다.
- 실행 설정은 필요한 부분만 코드와 이미지에서 분리하는 것이 관리상 유리하므로 `ConfigMap`을 최소 구성으로 두었다.

## 14. 선택 B. AWS 기초 이해

AWS 운영 아키텍처 구성도는 [aws-architecture.md](docs/aws-architecture.md) 에 정리했다.

사용한 AWS 서비스와 역할은 아래와 같다.

- `ECS Fargate`: generator, ingestion API, quality/analytics 배치 잡 실행
- `Amazon OpenSearch Service`: 이벤트 저장, 검색, 집계
- `Amazon S3`: 집계 JSON과 차트 이미지 저장
- `Amazon EventBridge`: generator와 배치 잡의 주기 실행 스케줄링
- `Amazon CloudWatch`: 로그와 상태 모니터링

서비스 역할 차이를 간단히 설명하면, ECS Fargate는 코드를 실행하는 환경이고, OpenSearch는 로그를 저장하고 조회하는 저장소이며, S3는 결과 파일을 보관하는 스토리지다. EventBridge는 주기 실행을 담당하고, CloudWatch는 운영 중 로그와 상태를 확인하는 역할을 한다.

가장 고민한 부분은 ingestion API 뒤에 durable queue를 둘지 여부였다. 현재 과제 구현은 in-memory queue로 충분하지만, 실제 AWS 운영 환경에서는 트래픽 급증과 장애 상황을 고려하면 SQS나 Kinesis를 중간에 두는 것이 더 안전하다. 이번 설계에서는 현재 코드 구조를 최대한 자연스럽게 확장하는 방향을 우선해 `API → OpenSearch` 흐름을 유지하고, 이후 queue를 추가하는 방향을 고려했다.
