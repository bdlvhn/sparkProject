// 8. 조인

val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500,250,100)),
    (2, "Michael Armbrust", 1, Seq(250,100))).
    toDF("id","name","graduate_program","spark_status")

val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.d", "EECS", "UC Berkeley")).
    toDF("id","degree","department","school")

val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")).
    toDF("id","status")

//  inner join
val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
val wrongJoinExpression = person.col("name") === graduateProgram.col("school")

person.join(graduateProgram,joinExpression).show()
// person.join(graduateProgram,wrongJoinExpression).show() // 잘못된 예시

var joinType = "inner"
person.join(graduateProgram,joinExpression,joinType).show()

// outer join
joinType = "outer"
person.join(graduateProgram,joinExpression,joinType).show()

// outer joins
joinType = "left_outer"
graduateProgram.join(person,joinExpression,joinType).show()

joinType = "right_outer"
person.join(graduateProgram,joinExpression,joinType).show()

 // semi join (필터같은 역할)
joinType = "left_semi"
graduateProgram.join(person,joinExpression,joinType).show()
val gradProgram2 = graduateProgram.union(Seq(
    (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())

// anti join
joinType = "left_anti"
graduateProgram.join(person,joinExpression,joinType).show()

// cross(cartesian) join
joinType = "cross"
graduateProgram.join(person,joinExpression,joinType).show()
person.crossJoin(graduateProgram).show()

// 8.11 조인 사용 시 문제점
// 복합 데이터타입의 조인
import org.apache.spark.sql.functions

person.withColumnRenamed("id","personId").
    join(sparkStatus, expr("array_contains(spark_status,id)")).show()

// 중복 컬럼명 처리
// 0. 문제 발생
val gradProgramDupe = graduateProgram.withColumnRenamed("id","graduate_program")
val joinExpr = gradProgramDupe.col("graduate_program") === person.col("graduate_program")
person.join(gradProgramDupe,joinExpr).show()
person.join(gradProgramDupe,joinExpr).select("graduate_program").show()

// 1. 다른 조인 표현식 사용
person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()

// 2. 조인 후 컬럼 제거
person.join(gradProgramDupe,joinExpr).drop(person.col("graduate_program")).
    select("graduate_program").show()

val joinExpr = person.col("graduate_program") === graduate_program.col("id")
person.join(graduateProgram, joinExpr).drop(graduate_program("id")).show()

// 3. 조인 전 컬럼명 변경
val gradProgram3 = graduateProgram.withColumnRenamed("id","grad_id")
val joinExpr = person.col("graduate_program") === gradProgram3.col("grad_id")
person.join(gradProgram3,joinExpr).show()

val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
person.join(graduateProgram,joinExpr).explain()
person.join(broadcast(graduateProgram),joinExpr).explain()