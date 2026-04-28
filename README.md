# liveklass

OpenSearch 기반 클릭스트림 이벤트 파이프라인 과제입니다.  
온라인 강의 탐색 서비스를 가정하고, 이벤트 생성 → 수집 → 저장 → 데이터 품질 점검까지의 흐름을 구현했습니다.

현재 구현 범위는 아래와 같습니다.

- Step 1. 이벤트 생성기 작성
- Step 2. 로그 저장
- 데이터 품질 점검 배치 잡
- OpenSearch Dashboards 연동

아직 진행 중인 범위는 아래와 같습니다.

- Step 3. 집계 분석 고도화
- Step 5. 시각화 결과 정리

## 1. 아키텍처

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
  └── data-quality-results
        ↓
OpenSearch Dashboards
```

## 2. 실행 방법

### 필요한 도구

- Docker
- Docker Compose

### 실행

```bash
docker compose up --build
```

### 확인

- Ingestion API health: `http://localhost:8000/health`
- OpenSearch: `http://localhost:9200`
- OpenSearch Dashboards: `http://localhost:5601`

예를 들어 저장 여부는 아래처럼 확인할 수 있습니다.

```bash
curl http://localhost:8000/health
curl http://localhost:9200/clickstream-events/_count
```

## 3. Step 1. 이벤트 생성기 설계

이 프로젝트의 이벤트 생성기는 온라인 강의/코스 탐색 서비스를 가정하고 설계했다.  
사용자가 강의 목록 페이지를 방문하고, 검색 결과에서 강의를 노출받고, 일부를 클릭하고, 경우에 따라 구매하거나 에러를 경험하는 흐름을 하나의 세션 단위로 생성한다.

### 생성한 이벤트 타입

본 과제에서는 아래 5가지 이벤트를 생성한다.

- `page_view`
- `impression`
- `click`
- `purchase`
- `error`

최소 2개 이상의 이벤트 타입 요구사항보다 조금 더 확장해서, 실제 서비스에서 자주 볼 수 있는 탐색 → 클릭 → 구매 흐름이 드러나도록 구성했다.

### 이벤트를 이렇게 설계한 이유

단순히 랜덤한 로그를 찍는 것보다, 사용자의 행동 흐름이 보이도록 이벤트를 설계하는 것이 이후 집계와 시각화에 더 적합하다고 판단했다.

- `page_view`: 사용자가 어떤 페이지에 진입했는지 확인하기 위한 기본 이벤트
- `impression`: 어떤 강의가 몇 번째 위치에 노출되었는지 기록하기 위한 이벤트
- `click`: 실제 클릭 행동을 기록하는 핵심 이벤트
- `purchase`: 클릭 이후 전환 행동을 보기 위한 이벤트
- `error`: 서비스 운영 중 발생 가능한 실패 상황을 분석하기 위한 이벤트

특히 `impression`과 `click`을 분리한 이유는 CTR(Click Through Rate) 같은 지표를 계산하기 위해서다.

### 클릭 생성 방식

클릭 이벤트는 완전한 무작위가 아니라, 실제 서비스와 비슷한 패턴을 반영하도록 설계했다.

클릭 확률은 아래 3가지 bias를 반영한다.

- `Position Bias`: 상위 랭크에 노출된 강의일수록 클릭 확률이 높음
- `Popularity Bias`: 이미 인기 있는 강의일수록 클릭 확률이 높음
- `Presentation Bias`: featured card, discount badge, live badge 같은 UI 표현이 클릭률에 영향

즉, 같은 강의라도 노출 위치와 UI 표현 방식에 따라 클릭될 가능성이 달라지도록 만들었다.

### 주요 필드 구성

모든 이벤트는 아래 공통 필드를 가진다.

- `event_id`
- `schema_version`
- `event_type`
- `user_id`
- `session_id`
- `event_time`
- `received_at`
- `device`

이벤트별 추가 필드는 다음과 같다.

- `page_view`: `page_url`
- `impression`: `query`, `item_id`, `rank`, `popularity_bucket`, `presentation_type`, `position_bias`
- `click`: `query`, `item_id`, `rank`, `popularity_bucket`, `presentation_type`, `position_bias`, `click_prob`
- `purchase`: `item_id`, `amount`
- `error`: `error_code`, `error_message`

### 생성 단위

이벤트는 세션 단위로 생성된다.

1. 사용자가 페이지에 방문한다 (`page_view`)
2. 검색 결과 목록이 노출된다 (`impression`)
3. 일부 결과가 클릭된다 (`click`)
4. 일부 클릭은 구매로 이어진다 (`purchase`)
5. 낮은 확률로 에러 이벤트도 발생한다 (`error`)

기본 설정에서는 여러 세션을 생성하며, 실행할 때마다 수백~수천 건 규모의 이벤트를 만들 수 있다.

## 4. Step 2. 로그 저장 설계

### 저장소 선택

로그 저장소는 OpenSearch를 선택했다.

선택 이유는 다음과 같다.

- 이벤트 로그는 반정형(semi-structured) 데이터라서 문서형 저장소와 잘 맞는다.
- 필드별 검색, 필터링, 집계가 쉬워서 이후 분석 단계로 연결하기 좋다.
- OpenSearch Dashboards를 함께 사용하면 저장된 데이터를 바로 탐색하고 시각적으로 확인할 수 있다.
- 이번 과제 규모에서는 복잡한 관계형 모델보다 이벤트 로그 중심 탐색과 집계가 더 중요하다고 판단했다.

### 저장 방식

이벤트는 `generator`가 `POST /events`로 ingestion API에 전송하고, ingestion API가 검증 후 OpenSearch에 bulk insert 한다.

```text
generator
  → ingestion API
  → validation
  → in-memory queue
  → OpenSearch bulk insert
```

즉, 단순히 JSON 파일을 쌓는 방식이 아니라 필드를 구분한 문서 형태로 저장하고, 각 필드에 명시적인 mapping을 적용했다.

### 인덱스 구성

현재 사용하는 인덱스는 아래 3개다.

- `clickstream-events`: 정상 이벤트 저장
- `dead-letter-events`: 검증 실패 이벤트 저장
- `data-quality-results`: 데이터 품질 점검 결과 저장

### 스키마 설명

`clickstream-events` 인덱스는 공통 이벤트 필드와 이벤트별 상세 필드를 분리해서 저장한다.  
`event_type`, `user_id`, `item_id`, `device` 같은 필드는 집계와 필터링이 자주 발생하므로 `keyword`로 두었고, `rank`, `amount`, `click_prob`, `position_bias`는 수치 집계를 위해 숫자 타입으로 저장했다.  
`event_time`, `received_at`는 시간 기준 분석을 위해 `date` 타입으로 저장했다.

주요 필드 타입은 아래와 같다.

| Field | Type |
| --- | --- |
| `event_id` | `keyword` |
| `schema_version` | `keyword` |
| `event_type` | `keyword` |
| `user_id` | `keyword` |
| `session_id` | `keyword` |
| `device` | `keyword` |
| `page_url` | `keyword` |
| `query` | `keyword` |
| `item_id` | `keyword` |
| `rank` | `integer` |
| `popularity_bucket` | `keyword` |
| `presentation_type` | `keyword` |
| `position_bias` | `float` |
| `click_prob` | `float` |
| `amount` | `float` |
| `error_code` | `keyword` |
| `error_message` | `text` |
| `event_time` | `date` |
| `received_at` | `date` |

추가 인덱스 구조는 아래와 같다.

- `dead-letter-events`
  - `event_id`
  - `payload`
  - `error_reason`
  - `failed_stage`
  - `created_at`

- `data-quality-results`
  - `check_name`
  - `status`
  - `failed_count`
  - `checked_at`
  - `details`

## 5. Validation과 Data Quality Check

이 프로젝트에서는 입력 검증과 데이터셋 정합성 점검을 분리했다.

- `validation`
  - ingestion 시점에 이벤트 1건씩 검사
  - 잘못된 이벤트는 `dead-letter-events`로 보냄
- `data quality check`
  - 저장된 전체 이벤트셋을 다시 검사
  - 결과는 `data-quality-results`에 저장

현재 구현한 데이터 품질 점검 항목은 아래와 같다.

- required field null check
- invalid event type count
- duplicate `event_id` count
- `rank` range check
- `click_prob` range check
- `position_bias` range check
- same `item_id/session_id/query` 기준 `click > impression` 여부

## 6. OpenSearch Dashboards

OpenSearch Dashboards를 함께 띄워서 저장된 이벤트를 직접 확인할 수 있다.

- URL: `http://localhost:5601`
- Dev Tools에서 `_count`, `_search`, `terms aggregation` 등을 바로 확인 가능

예시 쿼리:

```http
GET clickstream-events/_count
```

```http
GET clickstream-events/_search
{
  "size": 5,
  "sort": [
    { "event_time": "desc" }
  ]
}
```

## 7. 구현하면서 고민한 점

- 완전 랜덤 로그보다 실제 서비스처럼 보이는 클릭스트림을 만들기 위해 `page_view → impression → click → purchase` 흐름을 세션 단위로 구성했다.
- `impression`과 `click`을 분리해야 CTR, rank별 분석, bias 분석이 가능하다고 판단했다.
- 입력 검증과 데이터 품질 점검은 역할이 다르기 때문에 ingestion validation과 quality job을 분리했다.
- 과제 제출 환경을 고려해 `.env` 없이도 `docker compose up`만으로 동작하도록 기본값 중심으로 설정했다.
- OpenSearch는 로컬 실행 편의성을 위해 no-security 개발 설정으로 구성했다.

## 8. 현재 한계

- in-memory queue라서 프로세스 종료 시 메모리 버퍼는 유실될 수 있다.
- invalid event를 일부러 생성하는 로직은 아직 추가하지 않았다.
- aggregation / visualization 결과물은 아직 진행 중이다.

## 9. 이후 확장 방향

- `analytics/aggregate.py`에서 rank CTR, debiased CTR, cascade-style session 분석 구현
- `analytics/visualize.py` 또는 OpenSearch Dashboards 기반 시각화 정리
- production 환경에서는 in-memory queue 대신 Kafka/Kinesis 같은 durable queue 사용
- 데이터 품질 점검 실패 시 알림이나 재처리 흐름 추가
