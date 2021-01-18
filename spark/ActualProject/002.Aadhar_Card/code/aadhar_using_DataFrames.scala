case class AadharData(date:String, register:String, store:String, state:String,
    district:String, sub:String, pin:String, gender:String, age:Int, generated:Int,
    rejected:Int, mobile:String, email:String)

val schema = org.apache.spark.sql.Encoders.product[AadharData].schema

val data = spark.read.
    option("header","false").
    format("csv").
    schema(schema).
    load("/home/hoon/project/spark/ActualProject/002.Aadhar_Card/data/aadhar_data.csv").
    toDF()
data.createOrReplaceTempView("aadhar")
data.cache()

val kpi_01 = spark.sql("""
    select *
    from 
    (select *, row_number() over(partition by store order by date) as rownum from aadhar
    )
    where rownum <= 25
    """)
kpi_01.show()

/*KPI 01*/

val kpi_02_1 = spark.sql("""
    select register, count(*) as count
    from aadhar
    group by 1
    """)
kpi_02_1.show()

val kpi_02_2 = spark.sql("""
    select state, district, sub
    from aadhar
    group by 1,2,3
    order by 1,2,3
    """)

val kpi_02_2_1 = spark.sql("""
    select count(distinct state) as count
    from aadhar
    """)
kpi_02_2_1.show()

val kpi_02_2_2 = spark.sql("""
    select state, count(distinct district) as count
    from aadhar
    group by 1
    """)
kpi_02_2_2.show()

val kpi_02_2_3 = spark.sql("""
    select district, count(distinct sub) as count
    from aadhar
    group by 1
    """)
kpi_02_2_3.show()

val kpi_02_3 = spark.sql("""
    select state, gender, count(*) as count
    from aadhar
    group by 1,2
    order by 1,2
    """)
kpi_02_3.show()

/*KPI 02*/

val kpi_03_1 = spark.sql("""
    select state, sum(generated) as sum
    from aadhar
    group by 1
    order by 2 desc
    limit 3
    """)
kpi_03_1.show()

val kpi_03_2 = spark.sql("""
    select store, sum(generated) as sum
    from aadhar
    group by 1
    order by 2 desc
    limit 3
    """)
kpi_03_2.show()

val kpi_03_3 = spark.sql("""
    select count(*) as count
    from aadhar
    where mobile != 0 and email != 0
    """)
kpi_03_3.show()

val kpi_03_4 = spark.sql("""
    select district, sum(generated) as sum
    from aadhar
    group by 1
    order by 2 desc
    limit 3
    """)
kpi_03_4.show()

val kpi_03_5 = spark.sql("""
    select state, sum(generated) as sum
    from aadhar
    group by 1
    """)
kpi_03_5.show()

/*KPI 03*/

val dsMob = spark.sql("select age as age1, sum(generated) as sumMob from aadhar where mobile!=0 group by 1")
val dsNoMob = spark.sql("select age as age2, sum(generated) as sumNoMob from aadhar where mobile=0 group by 1")
val MobJoin = dsMob.join(dsNoMob,dsMob("age1")===dsNoMob("age2"),"inner")
MobJoin.createOrReplaceTempView("mobjoin")
val kpi_04_1 = spark.sql("""
    select age1 as age, sumMob, sumNoMob,
        100*(sumMob/(sumMob+sumNoMob)) as perctPeopleWithMobile
    from mobjoin
    order by 1""")
kpi_04_1.show()

val kpi_04_2 = spark.sql("select count(distinct pin) from aadhar")
kpi_04_2.show()


val kpi_04_3 = spark.sql("""
    select state, sum(rejected) as rejectSum
    from aadhar
    where state in ('Uttar Pradesh','Maharashtra')
    group by 1
    """)
kpi_04_3.show()

/*KPI 04*/

val kpi_05_1 = spark.sql("""
    select state,
        sum(generated) as total,
        sum(if(gender='M',generated,0)) as males,
        round(sum(if(gender='M',generated,0))/sum(generated)*100,2) as maleGenPer
    from aadhar
    group by 1
    order by 4 desc
    limit 3
    """)
kpi_05_1.show()

val kpi_05_2 = spark.sql("""
    select district,
        round(100*(sum(if(gender='F',rejected,0))/(sum(if(gender='F',rejected,0))+sum(if(gender='F',generated,0)))),2) as femaleRejPer
    from aadhar
    where state in ('Manipur','Arunachal Pradesh','Nagaland')
    group by 1
    order by 2 desc
    limit 3
    """)
kpi_05_2.show()

val kpi_05_3 = spark.sql("""
    select state,
        sum(generated) as total,
        sum(if(gender='F',generated,0)) as males,
        round(sum(if(gender='F',generated,0))/sum(generated)*100,2) as femaleGenPer
    from aadhar
    group by 1
    order by 4 desc
    limit 3
    """)
kpi_05_3.show()

val kpi_05_4 = spark.sql("""
    select district,
        round(100*(sum(if(gender='M',rejected,0))/(sum(if(gender='M',rejected,0))+sum(if(gender='M',generated,0)))),2) as maleRejPer
    from aadhar
    where state in ('Andaman and Nicobar Islands','Chhattisgarh','Goa')
    group by 1
    order by 2 desc
    limit 3
    """)
kpi_05_4.show()

def calcRange(num:Int):String = { 
      var range="none"
      if(num <=10) 
        range= "1-10"
      else if(num <=20) 
        range= "11-20"
      else if(num <=30) 
        range= "21-30"
      else if(num <=40) 
        range= "31-40"
      else if(num <=50) 
        range= "41-50"
      else if(num <=60) 
        range= "51-60"
      else if(num <=70) 
        range= "61-70"
      else if(num <=80) 
        range= "71-80"
      else if(num <=90) 
        range= "81-90"
      else if(num <=100) 
        range= "91-100"
      range  
      
    }
spark.udf.register("calcRange", calcRange _)

val kpi_05_5 = spark.sql("""
    select calcRange(age) as age_group,
    sum(generated) as ageGroupGen,
    sum(rejected) as ageGroupRej,
    round((sum(generated)/(sum(generated)+sum(rejected)))*100,2) as ageGroupAcptPer
    from aadhar
    group by 1
    order by 1
    """)
kpi_05_5.show()