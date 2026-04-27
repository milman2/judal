# judal

KRX 장 종료 후, 외국인, 기관의 매수/매도량을 크롤링하여 저장하는 프로그램입니다

# 가상환경

```shell
uv env

# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate

uv pip install exchange_calendars requests BeautifulSoup openpyxl tabulate

uv pip freeze > requirements.txt
```

## VSCode 기본 python 가상환경 설정/변경

- Ctrl + Shift + P -> Python: Select Interpreter -> .venv 선택
- 가상환경을 쓰고 싶지 않다면 시스템 기본 파이썬을 선택

- [Settings] - Python: Activate Environment - 체크 해제
  - Python : Select Interpreter 목록에서 Recommended 표시된 가상환경이 자동으로 활성화 되는 것 같다.

# Github Actions

- 매일 오후 3시 30분 이후(40분 정도) 자동으로 실행

# Gist

- 생성된 md 파일을 gist에 업로드하여 공유

## Secret 설정

- [Personal Access Token (classic)](https://github.com/settings/tokens) 생성 (권한: gist 체크)
  - Generate new token (classic)
    - 권한 check: gist
- repo의 Settings - Secrets and variables - Actions

  - New repository secret
    - GIST_TOKEN: 위에서 생성한 Personal Access Token 입력

# Releases

- 생성된 db3 파일을 release에 업로드하여 공유

# Github Artifacts
