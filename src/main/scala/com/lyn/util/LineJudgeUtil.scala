package com.lyn.util

object LineJudgeUtil {

  def line()= {

  }

 def matchData(data:String,dataMatch:String,name:String)= {
    if (data != null) {
     if (dataMatch != null && dataMatch != "" && dataMatch != "null") {
       if (data == dataMatch) {
         //匹配判决日期
         (1, name + "正常")
       } else {
         (3, name + "异常")
       }
     }else{
       (1,  name + "正常")
     }
   } else {
     if (dataMatch != null) {
       (1,  name + "正常")
     } else {
       (3,  name + "异常")
     }
   }
 }

  def judge(caseNumber: String, court: String, judgementDate: String, caseType: String, content: String) = {
    val caseNumberMath = RegexUtil.regexCaseNumber(content)
    val courtMatch = RegexUtil.regexCourt(content)
    val judgementDateMatch = RegexUtil.regexJudgementDate(content)
    val caseTypeMatch = RegexUtil.regexCaseType(content)
    //案号不为空
    val caseNumberFunc = if (caseNumber != null) {
      if (caseNumberMath != null) {
        if (caseNumber == caseNumberMath) {
          (1, "案号正常")
        } else {
          (3, "匹配异常")
        }
      } else {
        (1, "案号正常")
      }
    } else {
      //没有案号,提取到案号 第二条线
      if (caseNumberMath != null) {
        //提取到案号ok
        (1, "案号正常")
      } else {
        //没有提取到案号 判断是否是高院或者死刑复核等不带案号类
        //人工筛查,数据不做处理,存到新的表里面
        if (court.contains("高级人民法院") || content.contains("死刑复核")) {
          //是高院 提取法院匹配字典
          (2, "无需案号类")
        } else {
          (3, "匹配异常")
        }
      }
    }

    val caseResult = caseNumberFunc._1.toString.toInt
    val courtFunc = if (caseResult != 2) matchData(court, courtMatch, "法院") else (0,null)
    val courtResult = courtFunc.toString.substring(1, 2).toInt
    val judgementDateFunc = if (courtResult == 1) matchData(judgementDate, judgementDateMatch, "裁判日期") else (0,null)
    val judgementDateResult = judgementDateFunc.toString.substring(1, 2).toInt
    val caseTypeFunc = if (judgementDateResult == 1) matchData(caseType, caseTypeMatch, "文书类型") else (0,null)
    val caseTypeResult = caseTypeFunc.toString.substring(1, 2).toInt
    if(caseResult == 1 && courtResult == 1 && judgementDateResult == 1 && caseTypeResult == 1){
      1
    }else if(caseResult == 2 && courtResult == 1 && judgementDateResult == 1 && caseTypeResult == 1){
      2
    }else{
      3
    }

  }
}