# interest-seg

웹/앱 접속 호스트(사이트) 유사도 Network 기반 관심사 Segment 정의 및 식별

### HOW-TO-DO

가상환경 실행 
`conda activate ./venv`  

가상환경 종료 
`conda deactivate`  


### data-pipeline

configure setting
- input/output file name mapping (`/data/map_path_test.csv` 참고)
- 'BASE_MAP_PATH_FILE' 상수 수정 (`/script/utility.py` 파일 수정)
  - [ ] sys.argv 받아서 처리 필요

순차적인 코드 실행
1. Input 데이터 정제 
`python script/refine_graph_input.py`

2. Graph 분석 및 시각화 input 파일(Output) 생성 
`python script/process_graph_analysis.py`

