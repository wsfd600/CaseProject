package com.lyn.util

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.StringUtils


object RegexUtil{
  //解析案列

    //匹配法院
    def regexCourt(file: String) = {
      val courtMatch = "[\\u4E00-\\u9FA5]+[省|自治区]?[\\u4E00-\\u9FA5]+[市|州]*人民法院".r
      val court = (courtMatch findFirstIn file).mkString(",")
      court
    }
      /*
       * 案件字号
       *([(（][21].*?[）)].?[刑民执监行助协委司认请赔港财].*?[\\d]+)(?=(号|)),有的匹配不出来
       */
    def regexCaseNumber(file: String) = {
      val caseNumberMatch = """[(（][21].*?[）)][\u4E00-\u9FA5]*\d*[\u4E00-\u9FA5]*\d*号""".r
      val caseNumberBefore = (caseNumberMatch findFirstIn file).mkString(",")
      val caseNumberReplace = caseNumberBefore.replace(" ", "").replace("[", "(").replace("【", "(").replace("（", "(")
        .replace("]", ")").replace("　", "").replace("〔", "(").replace("〕", ")").replace("】", ")").replace("）", ")").replace("号)","号" ).replace(" ", "").replace("((", "(");
      val caseNumber = StringUtils.substringBefore(caseNumberReplace, "&nbsp<br>")
      caseNumber
    }
    //案件类型
    def regexCaseType(file: String) = {
      val caseTypeMatch = "[民刑]事".r
      val caseType = (caseTypeMatch findFirstIn file).mkString(",")
      caseType
    }

    //判决类型
    def regexJudgementType(file: String) = {
      val judgementTypeMatch = "(判\\s?\\　?决\\s?\\　?|裁\\s?\\　?定\\s?\\　?)书".r
      val judgementTypeBefore = (judgementTypeMatch findFirstIn file).mkString(",")
      val judgementType = judgementTypeBefore.replaceAll(" ", "").replaceAll("　", "")
      //println(judgement)
    }
    //案由
    //正文

    //案发时间
/*    val publishDateMatch = "\\d{4}年\\d{1,2}月\\d{1,2}日\\d{1,2}时?\\d{1,2}分?".r
    val publishDateBefore = (publishDateMatch findAllIn file).mkString(",")
    val publishDateMatch2 = "\\d{4}年\\d{1,2}月\\d{1,2}日".r
    val publishDate = (publishDateMatch2 findFirstIn publishDateBefore).mkString(",")*/
    //println(publishDate)

    /*
    *审理时间 时间转换
    * 二Ｏ一七年十一月二十七日转换成2017年11月27日
    * */
    def regexJudgementDate(file: String) = {
      val judgementDateMath = "[\\u4E00-\\u9FA5\\s][\\u4E00-\\u9FA5\\s]+年[\\u4E00-\\u9FA5\\s]+月[\\u4E00-\\u9FA5\\s]+日".r
      val judgementDateBefore = (judgementDateMath findAllIn file).mkString(",")
      val year = StringUtils.substringBefore(judgementDateBefore, "年")
      val yearBulider = new StringBuilder

      for (i <- year.split("")) {
        val date = MathDate(i)
        yearBulider ++= date
      }
    val monthOne = StringUtils.substringBefore(judgementDateBefore,"月")
    val monthBefore = StringUtils.substringAfter(monthOne,"年")
    val month = MathDate(monthBefore)

    val days = StringUtils.substringBefore(judgementDateBefore,"日")
    val dayBefore = StringUtils.substringAfter(days,"月")
    val day = MathDate(dayBefore)
    val judgementDate = yearBulider + month + day
    judgementDate
    }

  def regexReason(file: String) = {
    val reasonMatch = """[盗窃|姓名权|网络传播权|发明专利权|探望权|股权转让|股东出资|商品房销售合同|调解书|健康权|高度危险责任|公司票据|申请支付令督促民事|劳动报酬|使用权|劳动关系|合同诈骗罪驳回|受害责任|人格权|不当得利|合同无效|劳动|相邻关系|保险|理财|委托|抵押权|股权|合伙协议|故意伤害|公司|确认合同效力|非法吸收公众存款|加工合同|赡养|不正当|银行卡|网络|饲养动物|融资|股东|商品房预售|人民调解代理|房屋拆迁安置补偿|侵害|分家析产|著作权|公证债权|名誉权|财产损害|房屋买卖|提供劳务者致害|建设工程施工|公共场所管理人责任|返还原物|建设工程|离婚|承揽|医疗损害|机动车交通事故责任|合伙协议|信息公开|劳动争议|人事|争议|继承|故意伤害|证券虚假陈述责任|著作权|寻衅滋事|租赁|追偿权|企业承包经营|租赁|婚姻|所有权确认|劳动|买卖合同|生命权|金融借款|装饰装修|物业服务|供用热力|侵权|借贷|欠款|赔偿|身体权|排除妨害|信用卡|交通事故责任|居间合同|借贷]*.?纠纷?""".r
    val str = (reasonMatch findAllIn file).mkString(",")
    val matchs = if (str == "null" || str == "" || str == null) {
      val matcherror = """强奸|管辖裁定书|民事调解书|诈骗|执行决定书|补正笔误裁定书|执行裁定书|行政判决书|刑事判决书|刑事裁定书|民事判决书|民事裁定书|盗窃|敲诈勒索|聚众斗殴|劳动争议|寻性滋事|寻衅滋事|调解书|故意伤害|刑事判决书|民事裁定书|民事判决书|抚养判决|赔偿判决书|执行裁定书|行政裁定书|刑事决定书|抢劫|判决书|审查裁定书|无罪赔偿赔偿决定书|特别程序判决书|一案裁定书|司法救助裁定书|指定管辖决定书|执行审查裁定书|执行通知书|走私|贩卖|制造毒品|非法制造|储存枪支|弹药|爆炸物*.?""".r
       (matcherror findAllIn file).mkString(",").toString
    } else {
      str
    }
    //案件来源网址
    matchs
  }

  //数字大写匹配小写
  def MathDate(i: String): String = i match {
    case "○" => 0.toString
    case "Ｏ" => 0.toString
    case "〇" => 0.toString
    case "零" => 0.toString
    case "一" => 1.toString
    case "二" => 2.toString
    case "三" => 3.toString
    case "四" => 4.toString
    case "五" => 5.toString
    case "六" => 6.toString
    case "七" => 7.toString
    case "八" => 8.toString
    case "九" => 9.toString
    case "十" => 10.toString
    case "十一" => 11.toString
    case "十二" => 12.toString
    case "十三" => 13.toString
    case "十四" => 14.toString
    case "十五" => 15.toString
    case "十六" => 16.toString
    case "十七" => 17.toString
    case "十八" => 18.toString
    case "十九" => 19.toString
    case "二十" => 20.toString
    case "二十一" => 21.toString
    case "二十二" => 22.toString
    case "二十三" => 23.toString
    case "二十四" => 24.toString
    case "二十五" => 25.toString
    case "二十六" => 26.toString
    case "二十七" => 27.toString
    case "二十八" => 28.toString
    case "二十九" => 29.toString
    case "三十" => 30.toString
    case "三十一" => 31.toString
    case _ => "null"
  }

  def KeyIsExist(json: JSONObject, fi: Any):String = {
    if (json.containsKey(fi)) {
      json.get(fi).toString
    } else {
      null
    }
  }
}
