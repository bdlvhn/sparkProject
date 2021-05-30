## Spark Project - 1.3 Retail Data Analysis

> 목적 : 스파크로 데이터 파일을 읽어와서 단어 단위로 word count를 실행하는 jar 파일을 생성하고, 결과물을 도출한다.

## 폴더 구성
- code : 구현한 java 코드가 담긴 폴더
- input : 사용할 데이터파일이 담긴 폴더
- output : 결과물을 저장할 폴더

## 문제 설명
소매 구매 데이터 세트를 분석하고 아래 정의된 대로 KPI를 구현합니다.

1. 모든 스토어에 걸쳐 제품 범주별로 판매 내역을 계산합니다.
2. 모든 스토어에 걸친 스토어별 판매 내역을 계산합니다. 도시당 하나의 스토어가 있다고 가정합니다.
3. 모든 스토어에서 총 판매 값과 총 판매 수를 찾습니다.

## Sample Input Data

**Date Time City Product-Cat Sale-Value Payment-Mode**
```
2012-01-01 09:00 Fort Worth Women's Clothing 153.57 Visa
2012-01-01 09:00 San Jose Men's Clothing 214.05 Amex
2012-01-01 09:00 San Diego Music 66.08 Cash
2012-01-01 09:00 Pittsburgh Pet Supplies 493.51 Discover
2012-01-01 09:00 Omaha Children's Clothing 235.63 MasterCard
2012-01-01 09:00 Stockton Men's Clothing 247.18 MasterCard
2012-01-01 09:00 San Diego Cameras 379.6 Visa
2012-01-01 09:00 New York Consumer Electronics 296.8 Cash
```
