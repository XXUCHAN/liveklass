## Step 1. 이벤트 생성기 설계

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
