## Spark Project - 1.4 World Bank Analysis
> 목적 : 세계은행의 인구, 보건, 인터넷, GDP 등과 관련된 데이터 자료를 분석하고 아래에 언급된 문제들을 해결해야 한다.

## 폴더 구성
- code : 구현한 java 코드가 담긴 폴더
- input : 사용할 데이터파일이 담긴 폴더
- output : 결과물을 저장할 폴더

## 문제 설명
*소매 구매 데이터 세트를 분석하고 아래 정의된 대로 KPI를 구현합니다.*

1. 모든 스토어에 걸쳐 제품 범주별로 판매 내역을 계산합니다.
2. 모든 스토어에 걸친 스토어별 판매 내역을 계산합니다. 도시당 하나의 스토어가 있다고 가정합니다.
3. 모든 스토어에서 총 판매 값과 총 판매 수를 찾습니다.

## Data Format

+ Country Name
+ Date
+ "Transit: Railways, (million passenger-km)"
+ "Transit: Passenger cars (per 1,000 people)"
+ Business: Mobile phone subscribers
+ Business: Internet users (per 100 people)
+ "Health: Mortality
+ under-5 (per 1,000 live births)"
+ Health: Health expenditure per capita (current US$)
+ "Health: Health expenditure, total (% GDP)"
+ Population: Total (count)
+ Population: Urban (count)
+ "Population:: Birth rate, crude (per 1,000)"
+ "Health: Life expectancy at birth, female (years)"
+ "Health: Life expectancy at birth, male (years)"
+ "Health: Life expectancy at birth, total (years)"
+ Population: Ages 0-14 (% of total)
+ Population: Ages 15-64 (% of total)
+ Population: Ages 65+ (% of total)
+ Finance: GDP (current US$)
+ Finance: GDP per capita (current US$)

## Sample Data
```
Afghanistan,7/1/2000,0,,0,,151,11,8,"25,950,816","5,527,524",51,45,45,45,48,50,2,,
Afghanistan,7/1/2001,0,,0,0,150,11,9,"26,697,430","5,771,984",50,46,45,46,48,50,2,"2,461,666,315",92
Afghanistan,7/1/2002,0,,"25,000",0,150,22,7,"27,465,525","6,025,936",49,46,46,46,48,50,2,"4,338,907,579",158
Afghanistan,7/1/2003,0,,"200,000",0,151,25,8,"28,255,719","6,289,723",48,46,46,46,48,50,2,"4,766,127,272",169
Afghanistan,7/1/2004,0,,"600,000",0,150,30,9,"29,068,646","6,563,700",47,46,46,46,48,50,2,"5,704,202,651",196
Afghanistan,7/1/2005,0,,"1,200,000",1,151,33,9,"29,904,962","6,848,236",47,47,47,47,48,50,2,"6,814,753,581",228
Afghanistan,7/1/2006,0,11,"2,520,366",2,151,24,7,"30,751,661","7,158,987",46,47,47,47,48,50,2,"7,721,931,671",251
Afghanistan,7/1/2007,0,18,"4,668,096",2,150,29,7,"31,622,333","7,481,844",45,47,47,47,47,50,2,"9,707,373,721",307
Afghanistan,7/1/2008,0,19,"7,898,909",2,150,32,7,"32,517,656","7,817,245",45,48,47,48,47,51,2,"11,940,296,131",367
Afghanistan,7/1/2009,0,21,"12,000,000",3,149,34,8,"33,438,329","8,165,640",44,48,48,48,47,51,2,"14,213,670,485",425
Afghanistan,7/1/2010,0,,"13,000,000",4,149,38,8,"34,385,068","8,527,497",44,48,48,48,46,51,2,"17,243,112,604",501
```

## 문제 정의
1. 최고 도시 인구 - 최고 도시 인구 국가
2. 인구가 가장 많은 국가 - 인구의 내림차순 국가 목록
3. 최고 인구 증가 - 지난 10년 동안 가장 높은 인구 증가율을 보인 국가
4. GDP 성장률 최고치 - 2009년부터 2010년까지 GDP 성장률이 가장 높은 나라 목록 내림차순
5. 인터넷 사용량 증가 - 지난 10년 동안 인터넷 사용량이 가장 많이 증가한 국가
6. 최연소 국가 - 연간 최연소 국가 분포
