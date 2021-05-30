## Spark Project 1 - Set-top box

> 목적 : 셋탑박스 데이터를 분석하고 이 데이터에 대한 이해를 높이는 것이 목적이다. 이 데이터에는 채널 튜닝, 지속시간, 동영상 검색, VOD(주문형 비디오)를 이용한 동영상 구매 등 사용자의 활동에 대한 세부 정보가 담겨 있다.
> 스파크를 사용하여 데이터를 처리하고 KPI 섹션에 나열된 문제 해결하는 과정으로 이루어진다.

## 1. input data
```
DataCompany provides training on cutting edge technologies
DataCompany is the leading training provider, we have trained 1000s of candidates
Training focuses on practical aspects which industry needs rather than theoretical knowledge
...
```

## 2. Input to Mapper

**key	value**
```
0	DataCompany provides training on cutting edge technologies
57	DataCompany is the leading training provider, we have trained 1000s of candidates
108	Training focuses on practical aspects which industry needs rather than theoretical knowledge
..
```

## 3. Processing (map) (custom business logic)

**count all the words**
```
DataCompany	1
provides	1
training	1
on		1
cutting		1
edge		1
technologies	1
```

## 4. Output from the mapper (write to local disk)

**key value**
```
DataCompany	1
provides	1
training	1
on		1
cutting		1
edge		1
technologies	1
...
```

## 5. Input to reducer (All the values corresponding to same key goes to same reducer)

**key values**
```
DataCompany	[1,1,1,1,1......]
training	[1,1,1,1,1......]
...
```

## 6. Processing in reducer (custom business logic)

**sum all the counts**
```
330
```

## 7. Output from the reducer (Final output)

**key value**
```
DataCompany	330
training	500
technologies	25
...
```
