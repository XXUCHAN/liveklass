# liveklass

OpenSearch 기반 클릭스트림 이벤트 파이프라인 과제입니다.  
온라인 강의 탐색 서비스를 가정하고 이벤트 생성, 수집, 저장, 데이터 품질 점검까지 구현했습니다.

현재 구현 범위는 아래와 같습니다.

- Step 1. 이벤트 생성기 작성
- Step 2. 로그 저장
- Docker Compose 기반 실행 환경
- 저장 후 데이터 품질 점검
- OpenSearch Dashboards 연동

아직 남은 작업은 아래와 같습니다.

- Step 3. 집계 분석
- Step 5. 결과 시각화 정리

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
  ├── data-quality-results
  └── quality-checkpoints
        ↓
OpenSearch Dashboards
```

## 2. 실행 방법

### 필요한 도구

- Docker
- Docker Compose

### 실행

```bash
docker compose up
```

### 확인

```bash
curl http://localhost:8000/health
curl http://localhost:9200/clickstream-events/_count
```

접속 주소:

- Ingestion API: `http://localhost:8000/health`
- OpenSearch: `http://localhost:9200`
- OpenSearch Dashboards: `http://localhost:5601`

## 3. 이벤트 설계

이 프로젝트의 이벤트 생성기는 온라인 강의/코스 탐색 서비스를 가정했다.  
사용자가 페이지에 진입하고, 강의 목록을 노출받고, 일부를 클릭하고, 경우에 따라 구매하거나 에러를 경험하는 흐름을 세션 단위로 생성한다.

### 이벤트 타입

- `page_view`
- `impression`
- `click`
- `purchase`
- `error`

`impression`과 `click`을 분리한 이유는 CTR 같은 분석 지표를 계산하기 위해서다.

### 클릭 생성 방식

클릭은 완전한 무작위가 아니라 아래 3가지 bias를 반영한다.

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

### 이벤트별 주요 필드

- `page_view`: `page_url`
- `impression`: `query`, `item_id`, `rank`, `popularity_bucket`, `presentation_type`, `position_bias`
- `click`: `query`, `item_id`, `rank`, `popularity_bucket`, `presentation_type`, `position_bias`, `click_prob`
- `purchase`: `item_id`, `amount`
- `error`: `error_code`, `error_message`

## 4. 저장소 설계

### 저장소 선택

저장소는 OpenSearch를 선택했다.

선택 이유는 아래와 같다.

- 이벤트 로그는 반정형 데이터라 문서형 저장소와 잘 맞는다.
- 필드별 검색, 필터링, 집계가 쉽다.
- OpenSearch Dashboards로 저장된 데이터를 바로 확인할 수 있다.
- 이번 과제 범위에서는 PostgreSQL보다 로그 탐색과 집계 중심 구조가 더 적합하다고 판단했다.

### 저장 방식

이벤트는 `generator`가 `POST /events`로 ingestion API에 전송하고, ingestion API가 validation 후 OpenSearch에 bulk insert 한다.

잘못된 이벤트는 `dead-letter-events` 인덱스로 분리 저장한다.

### 인덱스 구성

- `clickstream-events`: 정상 이벤트 저장
- `dead-letter-events`: validation 실패 이벤트 저장
- `data-quality-results`: 품질 점검 결과 저장
- `quality-checkpoints`: 증분 품질 점검 체크포인트 저장

## 5. 스키마 설명

`clickstream-events`는 공통 이벤트 필드와 이벤트별 상세 필드를 분리해서 저장한다.  
`event_type`, `user_id`, `item_id`, `device` 같은 필드는 필터링과 집계를 위해 `keyword`로 두었고, `rank`, `amount`, `click_prob`, `position_bias`는 수치 계산을 위해 숫자 타입으로 저장했다.  
`event_time`, `received_at`, `ingested_at`은 시간 기준 분석과 증분 품질 점검을 위해 `date` 타입으로 저장했다.

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
| `ingested_at` | `date` |
| `arrival_lag_seconds` | `float` |
| `is_late_arrival` | `boolean` |

## 6. Validation과 Data Quality Check

이 프로젝트에서는 입력 검증과 저장 후 정합성 점검을 분리했다.

- `validation`: ingestion 시점에 이벤트 1건씩 검사하고, 실패한 이벤트는 `dead-letter-events`로 저장한다.
- `data quality check`: 저장된 이벤트를 다시 검사하고, 결과는 `data-quality-results`에 저장한다.
- `data quality check`는 체크포인트 기반 증분 검사 방식으로 동작한다.

현재 구현한 품질 점검 항목은 아래와 같다.

- required field null check
- invalid event type check
- duplicate `event_id` check
- `rank` range check
- `click_prob` range check
- `position_bias` range check
- same `item_id/session_id/query` 기준 `click > impression` 여부
- late-arriving event 비율 점검

## 7. Dashboards 확인

OpenSearch Dashboards에서 저장된 이벤트와 품질 점검 결과를 직접 확인할 수 있다.

- `http://localhost:5601`

예시 쿼리:

```http
GET clickstream-events/_count
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

## 8. 구현하면서 고민한 점

- 완전 랜덤 로그보다 실제 사용자 흐름처럼 보이는 클릭스트림을 만드는 것이 더 중요하다고 판단해 세션 기반으로 이벤트를 설계했다.
- `impression`과 `click`을 분리해야 이후 CTR 분석과 rank 기반 분석이 가능하다고 봤다.
- 저장 전 validation과 저장 후 quality check는 역할이 다르기 때문에 분리하는 쪽이 더 자연스럽다고 판단했다.
- 품질 점검은 전체 스캔보다 `ingested_at` 기반 증분 검사와 체크포인트 방식이 더 적절하다고 판단했다.
- 제출 환경을 고려해 `.env` 없이도 `docker compose up`으로 실행되도록 기본값 중심으로 구성했다.

## 9. 한계

- in-memory queue라서 프로세스 종료 시 메모리 버퍼는 유실될 수 있다.
- aggregation과 visualization 결과물은 아직 구현 중이다.
- invalid event를 의도적으로 대량 생성하는 시나리오는 아직 추가하지 않았다.
