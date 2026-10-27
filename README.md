# YourCareer 직업·채용 정보 수집

호주 YourCareer의 직업 정보와 채용 공고를 수집하고, 직업·공고 정보를 연결해 Google Sheets에 정리하는 인턴 업무 저장소입니다.

Seeka Technology 인턴 기간(2025.01–05)에 작성한 코드입니다.

**시작점:** [직업 목록](scripts/occupation_scrapping.py) · [공고 목록](scripts/vacancy_scrapping_1.py) · [결과 결합](scripts/occ_vac_compile_1.py)

## 파일을 읽는 순서

```text
직업 목록 → 직업 상세 ─┐
                      ├→ 직업·공고 결과 결합
공고 목록 → 공고 상세 ─┘
```

| 단계 | 파일 | 역할 |
| --- | --- | --- |
| 직업 목록 | [occupation_scrapping.py](scripts/occupation_scrapping.py) | 직업 목록과 식별 정보 수집 |
| 직업 상세 | [occupation_detail_1.py](scripts/occupation_detail_1.py) 및 번호별 파일 | 직업별 상세 정보 수집 |
| 공고 목록 | [vacancy_scrapping_1.py](scripts/vacancy_scrapping_1.py) 및 번호별 파일 | 채용 공고 목록 수집 |
| 공고 상세 | [vacancy_detail_1.py](scripts/vacancy_detail_1.py) 및 번호별 파일 | 공고 상세 정보 수집 |
| 결과 결합 | [occ_vac_compile_1.py](scripts/occ_vac_compile_1.py) 및 번호별 파일 | 직업·공고 결과 연결 |
| 시트·작업 관리 | [google_form_package.py](scripts/google_form_package.py), [process_handler.py](scripts/process_handler.py) | 시트 연결과 진행 관리 |

번호가 붙은 파일은 동일 업무의 처리 범위를 나눈 구성입니다. 각각을 별도 프로젝트나 별도 성과로 세지 않습니다.

## 실행 준비

Python, Chrome·ChromeDriver, Google Sheets 접근 권한과 서비스 계정이 필요합니다.

```bash
python -m pip install -r requirements.txt
```

전체 실행 순서는 [workflow 기록](.github/workflows/scrapping.yaml)에 있습니다. 목록·상세·결합 단계가 시트를 통해 연결되므로, 단일 파일 실행 전 해당 단계의 입력 시트와 이전 단계 결과를 준비해야 합니다.

## 현재 자동화 설정

현재 workflow 파일은 전체가 주석 처리돼 있습니다. 파일에 기록된 일일 스케줄은 당시 실행 설정이며, 현재 수집 중이라는 의미는 아닙니다.

## 인증과 저장 대상

[.env.example](.env.example)을 참고해 셸 환경 변수로 설정합니다. 파일은 자동으로 로드되지 않습니다.

| 변수 | 준비할 값 |
| --- | --- |
| `SERVICE_ACCOUNT_KEY` | 테스트 시트에 접근할 서비스 계정 JSON |
| `SPREADSHEET_URL` | 본인이 관리하는 HTTPS Google Sheets URL |

서비스 계정에 테스트 시트를 공유하고, 실행할 스크립트의 워크시트·입력 열을 먼저 준비합니다. 실행하면 실제 외부 수집과 시트 쓰기가 발생합니다.

시트 생성·초기화 함수는 기존 데이터를 지울 수 있습니다. 번호가 붙은 파일을 모두 순서대로 실행하는 방식이 아니며, 분할 구간과 시트 이름을 확인한 파일만 테스트 시트에서 실행합니다.

## 코드와 자료의 범위

업무 시트·인증 파일·수집 원본은 실행 예제로 제공하지 않습니다. 생성 결과와 인증 파일은 버전 관리에서 제외하며, 제외 규칙은 과거 이력을 제거하지 않습니다.

이번 정리에서 실행 코드와 보조 모듈을 `scripts/`로 옮겼습니다. 번호별 작업 구간과 기존 처리 로직은 유지합니다. 의존성과 설정 예시는 루트에 두며, 실행 명령의 기준 경로는 저장소 루트입니다.

설정과 입력 시트를 준비한 뒤 시작 단계만 실행하는 예:

```bash
python scripts/occupation_scrapping.py
```

시트 주소는 `https://docs.google.com/spreadsheets/d/…` 형식이어야 합니다. 누락된 설정·잘못된 URL·유효하지 않은 서비스 계정 JSON은 연결 전에 거부하며, 오류 메시지에 JSON 원문을 넣지 않습니다.

외부 수집 없이 설정·인증 처리를 검사하려면 의존성을 설치한 뒤 `python -m unittest discover -s tests`를 실행합니다.
