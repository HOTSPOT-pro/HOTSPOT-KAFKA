# Backend (Spring/Java) Code Review Style Guide

## 적용 범위
- Spring Boot 백엔드 모듈 전체
- PR 리뷰는 MUST 항목 위반 시 변경 요청을 기본으로 한다.

---

## 1) API/레이어링
### MUST
- Controller의 입출력은 DTO만 사용하고 Entity 직접 노출 금지
- Controller는 요청 검증/라우팅만 담당. 비즈니스 로직은 Service로 이동
- 요청 DTO는 @Valid 기반 검증을 사용하고, 수동 null-check 남발 금지

### SHOULD
- DTO/Entity 변환은 Mapper(예: MapStruct 또는 Converter)로 분리
- 응답 DTO는 필요한 필드만 노출 (민감정보/내부키/상태값 주의)

---

## 2) 예외/에러 처리
### MUST
- 의미 없는 RuntimeException 남발 금지: 도메인 예외 계층(예: BusinessException + ErrorCode)로 분리
- 4xx(클라이언트 오류) vs 5xx(서버 오류) 구분해서 응답
- @RestControllerAdvice로 예외→응답 매핑을 일원화

### SHOULD
- 에러 응답 포맷 통일(예: {code, message, traceId, details})
- Validation 오류는 필드 단위 에러(details) 포함

---

## 3) 트랜잭션/DB/JPA
### MUST
- @Transactional은 Service 계층에 선언 (Repository에 비즈니스 트랜잭션 로직 두지 않기)
- N+1 가능성이 있으면 “어떤 연관관계/조회 패턴 때문에”인지 근거를 남기고 수정 제안 포함

### SHOULD
- 조회 전용은 readOnly=true 사용 고려
- N+1 대응 우선순위:
    1) fetch join / EntityGraph
    2) DTO projection
    3) batch size 설정(필요 시)

---

## 4) 코드 품질
### MUST
- 메서드/변수 이름은 역할이 드러나게(약어 남발 금지)
- 매직넘버/문자열 상수는 상수화(의미 있는 이름으로)

### SHOULD
- 로그는 System.out 금지, 로거 사용 + PII 로깅 금지
- “왜 이런 선택을 했는지”가 중요한 부분은 짧게라도 주석/PR 설명에 남기기

---

## 5) 테스트
### MUST
- 신규 로직은 단위 테스트 1개 이상
- 경계값/예외 케이스 1개 이상 포함

### SHOULD
- 테스트는 given-when-then 구조 + 의미 있는 테스트명
- 외부 연동은 mocking 기준을 명확히(무엇을 단위로 검증하는지)

---

## 6) 리뷰 코멘트 작성 규칙
- 코멘트는 (문제) → (왜 문제인지) → (권장 수정안/대안) 순서로 작성
- 가능하면 수정 예시 코드(짧게) 또는 적용 포인트를 함께 제시
