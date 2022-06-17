from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, TimestampType

WORKER_SCHEMA = StructType(
    fields=[
        StructField("period", StringType(), True),
        StructField("establishmentid", IntegerType(), True),
        StructField("tribalid", IntegerType(), True),
        StructField("tribalid_worker", IntegerType(), True),
        StructField("parentid", IntegerType(), True),
        StructField("orgid", IntegerType(), True),
        StructField("nmdsid", StringType(), True),
        StructField("workerid", IntegerType(), True),
        StructField("wrkglbid", StringType(), True),
        StructField("wkplacestat", IntegerType(), True),
        StructField("createddate", TimestampType(), True),
        StructField("updateddate", TimestampType(), True),
        StructField("savedate", TimestampType(), True),
        StructField("cqcpermission", IntegerType(), True),
        StructField("lapermission", IntegerType(), True),
        StructField("regtype", IntegerType(), True),
        StructField("providerid", StringType(), True),
        StructField("locationid", StringType(), True),
        StructField("esttype", IntegerType(), True),
        StructField("regionid", IntegerType(), True),
        StructField("cssr", IntegerType(), True),
        StructField("lauthid", IntegerType(), True),
        StructField("mainstid", IntegerType(), True),
        StructField("emplstat", IntegerType(), True),
        StructField("mainjrid", IntegerType(), True),
        StructField("strtdate", TimestampType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", IntegerType(), True),
        StructField("disabled", IntegerType(), True),
        StructField("ethnicity", IntegerType(), True),
        StructField("isbritish", IntegerType(), True),
        StructField("nationality", IntegerType(), True),
        StructField("britishcitizen", IntegerType(), True),
        StructField("borninuk", IntegerType(), True),
        StructField("countryofbirth", IntegerType(), True),
        StructField("yearofentry", IntegerType(), True),
        StructField("homeregionid", IntegerType(), True),
        StructField("homecssrid", IntegerType(), True),
        StructField("homelauthid", IntegerType(), True),
        StructField("distwrkk", FloatType(), True),
        StructField("scerec", IntegerType(), True),
        StructField("startsec", IntegerType(), True),
        StructField("startage", IntegerType(), True),
        StructField("dayssick", FloatType(), True),
        StructField("dayssick_changedate", TimestampType(), True),
        StructField("dayssick_savedate", TimestampType(), True),
        StructField("zerohours", IntegerType(), True),
        StructField("averagehours", FloatType(), True),
        StructField("conthrs", FloatType(), True),
        StructField("salaryint", IntegerType(), True),
        StructField("salary", FloatType(), True),
        StructField("hrlyrate", FloatType(), True),
        StructField("pay_changedate", TimestampType(), True),
        StructField("pay_savedate", TimestampType(), True),
        StructField("ccstatus", IntegerType(), True),
        StructField("apprentice", IntegerType(), True),
        StructField("scqheld", IntegerType(), True),
        StructField("levelscqheld", IntegerType(), True),
        StructField("nonscqheld", IntegerType(), True),
        StructField("levelnonscqheld", IntegerType(), True),
        StructField("listqualsachflag", IntegerType(), True),
        StructField("listhiqualev", IntegerType(), True),
        StructField("jd16registered", IntegerType(), True),
        StructField("amhp", IntegerType(), True),
        StructField("trainflag", IntegerType(), True),
        StructField("flujab2020", IntegerType(), True),
        StructField("derivedfrom_hasbulkuploaded", IntegerType(), True),
        StructField("previous_pay", FloatType(), True),
        StructField("previous_mainjrid", IntegerType(), True),
        StructField("version", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("import_date", TimestampType(), True),
        
        StructField("tr01flag", IntegerType(), True),
        StructField("tr01latestdate", IntegerType(), True),
        StructField("tr01count", IntegerType(), True),
        StructField("tr01ac", IntegerType(), True),
        StructField("tr01nac", IntegerType(), True),
        StructField("tr01dn", IntegerType(), True),

        StructField("tr02flag", IntegerType(), True),
        StructField("tr02latestdate", IntegerType(), True),
        StructField("tr02count", IntegerType(), True),
        StructField("tr02ac", IntegerType(), True),
        StructField("tr02nac", IntegerType(), True),
        StructField("tr02dn", IntegerType(), True),

        StructField("tr05flag", IntegerType(), True),
        StructField("tr05latestdate", IntegerType(), True),
        StructField("tr05count", IntegerType(), True),
        StructField("tr05ac", IntegerType(), True),
        StructField("tr05nac", IntegerType(), True),
        StructField("tr05dn", IntegerType(), True),

        StructField("tr06flag", IntegerType(), True),
        StructField("tr06latestdate", IntegerType(), True),
        StructField("tr06count", IntegerType(), True),
        StructField("tr06ac", IntegerType(), True),
        StructField("tr06nac", IntegerType(), True),
        StructField("tr06dn", IntegerType(), True),

        StructField("tr07flag", IntegerType(), True),
        StructField("tr07latestdate", IntegerType(), True),
        StructField("tr07count", IntegerType(), True),
        StructField("tr07ac", IntegerType(), True),
        StructField("tr07nac", IntegerType(), True),
        StructField("tr07dn", IntegerType(), True),

        StructField("tr08flag", IntegerType(), True),
        StructField("tr08latestdate", IntegerType(), True),
        StructField("tr08count", IntegerType(), True),
        StructField("tr08ac", IntegerType(), True),
        StructField("tr08nac", IntegerType(), True),
        StructField("tr08dn", IntegerType(), True),

        StructField("tr09flag", IntegerType(), True),
        StructField("tr09latestdate", IntegerType(), True),
        StructField("tr09count", IntegerType(), True),
        StructField("tr09ac", IntegerType(), True),
        StructField("tr09nac", IntegerType(), True),
        StructField("tr09dn", IntegerType(), True),

        StructField("tr10flag", IntegerType(), True),
        StructField("tr10latestdate", IntegerType(), True),
        StructField("tr10count", IntegerType(), True),
        StructField("tr10ac", IntegerType(), True),
        StructField("tr10nac", IntegerType(), True),
        StructField("tr10dn", IntegerType(), True),

        StructField("tr11flag", IntegerType(), True),
        StructField("tr11latestdate", IntegerType(), True),
        StructField("tr11count", IntegerType(), True),
        StructField("tr11ac", IntegerType(), True),
        StructField("tr11nac", IntegerType(), True),
        StructField("tr11dn", IntegerType(), True),

        StructField("tr12flag", IntegerType(), True),
        StructField("tr12latestdate", IntegerType(), True),
        StructField("tr12count", IntegerType(), True),
        StructField("tr12ac", IntegerType(), True),
        StructField("tr12nac", IntegerType(), True),
        StructField("tr12dn", IntegerType(), True),

        StructField("tr13flag", IntegerType(), True),
        StructField("tr13latestdate", IntegerType(), True),
        StructField("tr13count", IntegerType(), True),
        StructField("tr13ac", IntegerType(), True),
        StructField("tr13nac", IntegerType(), True),
        StructField("tr13dn", IntegerType(), True),

        StructField("tr14flag", IntegerType(), True),
        StructField("tr14latestdate", IntegerType(), True),
        StructField("tr14count", IntegerType(), True),
        StructField("tr14ac", IntegerType(), True),
        StructField("tr14nac", IntegerType(), True),
        StructField("tr14dn", IntegerType(), True),

        StructField("tr15flag", IntegerType(), True),
        StructField("tr15latestdate", IntegerType(), True),
        StructField("tr15count", IntegerType(), True),
        StructField("tr15ac", IntegerType(), True),
        StructField("tr15nac", IntegerType(), True),
        StructField("tr15dn", IntegerType(), True),

        StructField("tr16flag", IntegerType(), True),
        StructField("tr16latestdate", IntegerType(), True),
        StructField("tr16count", IntegerType(), True),
        StructField("tr16ac", IntegerType(), True),
        StructField("tr16nac", IntegerType(), True),
        StructField("tr16dn", IntegerType(), True),

        StructField("tr17flag", IntegerType(), True),
        StructField("tr17latestdate", IntegerType(), True),
        StructField("tr17count", IntegerType(), True),
        StructField("tr17ac", IntegerType(), True),
        StructField("tr17nac", IntegerType(), True),
        StructField("tr17dn", IntegerType(), True),

        StructField("tr18flag", IntegerType(), True),
        StructField("tr18latestdate", IntegerType(), True),
        StructField("tr18count", IntegerType(), True),
        StructField("tr18ac", IntegerType(), True),
        StructField("tr18nac", IntegerType(), True),
        StructField("tr18dn", IntegerType(), True),

        StructField("tr19flag", IntegerType(), True),
        StructField("tr19latestdate", IntegerType(), True),
        StructField("tr19count", IntegerType(), True),
        StructField("tr19ac", IntegerType(), True),
        StructField("tr19nac", IntegerType(), True),
        StructField("tr19dn", IntegerType(), True),

        StructField("tr20flag", IntegerType(), True),
        StructField("tr20latestdate", IntegerType(), True),
        StructField("tr20count", IntegerType(), True),
        StructField("tr20ac", IntegerType(), True),
        StructField("tr20nac", IntegerType(), True),
        StructField("tr20dn", IntegerType(), True),

        StructField("tr21flag", IntegerType(), True),
        StructField("tr21latestdate", IntegerType(), True),
        StructField("tr21count", IntegerType(), True),
        StructField("tr21ac", IntegerType(), True),
        StructField("tr21nac", IntegerType(), True),
        StructField("tr21dn", IntegerType(), True),

        StructField("tr22flag", IntegerType(), True),
        StructField("tr22latestdate", IntegerType(), True),
        StructField("tr22count", IntegerType(), True),
        StructField("tr22ac", IntegerType(), True),
        StructField("tr22nac", IntegerType(), True),
        StructField("tr22dn", IntegerType(), True),

        StructField("tr23flag", IntegerType(), True),
        StructField("tr23latestdate", IntegerType(), True),
        StructField("tr23count", IntegerType(), True),
        StructField("tr23ac", IntegerType(), True),
        StructField("tr23nac", IntegerType(), True),
        StructField("tr23dn", IntegerType(), True),

        StructField("tr25flag", IntegerType(), True),
        StructField("tr25latestdate", IntegerType(), True),
        StructField("tr25count", IntegerType(), True),
        StructField("tr25ac", IntegerType(), True),
        StructField("tr25nac", IntegerType(), True),
        StructField("tr25dn", IntegerType(), True),

        StructField("tr26flag", IntegerType(), True),
        StructField("tr26latestdate", IntegerType(), True),
        StructField("tr26count", IntegerType(), True),
        StructField("tr26ac", IntegerType(), True),
        StructField("tr26nac", IntegerType(), True),
        StructField("tr26dn", IntegerType(), True),

        StructField("tr27flag", IntegerType(), True),
        StructField("tr27latestdate", IntegerType(), True),
        StructField("tr27count", IntegerType(), True),
        StructField("tr27ac", IntegerType(), True),
        StructField("tr27nac", IntegerType(), True),
        StructField("tr27dn", IntegerType(), True),

        StructField("tr28flag", IntegerType(), True),
        StructField("tr28latestdate", IntegerType(), True),
        StructField("tr28count", IntegerType(), True),
        StructField("tr28ac", IntegerType(), True),
        StructField("tr28nac", IntegerType(), True),
        StructField("tr28dn", IntegerType(), True),

        StructField("tr29flag", IntegerType(), True),
        StructField("tr29latestdate", IntegerType(), True),
        StructField("tr29count", IntegerType(), True),
        StructField("tr29ac", IntegerType(), True),
        StructField("tr29nac", IntegerType(), True),
        StructField("tr29dn", IntegerType(), True),

        StructField("tr30flag", IntegerType(), True),
        StructField("tr30latestdate", IntegerType(), True),
        StructField("tr30count", IntegerType(), True),
        StructField("tr30ac", IntegerType(), True),
        StructField("tr30nac", IntegerType(), True),
        StructField("tr30dn", IntegerType(), True),

        StructField("tr31flag", IntegerType(), True),
        StructField("tr31latestdate", IntegerType(), True),
        StructField("tr31count", IntegerType(), True),
        StructField("tr31ac", IntegerType(), True),
        StructField("tr31nac", IntegerType(), True),
        StructField("tr31dn", IntegerType(), True),

        StructField("tr32flag", IntegerType(), True),
        StructField("tr32latestdate", IntegerType(), True),
        StructField("tr32count", IntegerType(), True),
        StructField("tr32ac", IntegerType(), True),
        StructField("tr32nac", IntegerType(), True),
        StructField("tr32dn", IntegerType(), True),

        StructField("tr33flag", IntegerType(), True),
        StructField("tr33latestdate", IntegerType(), True),
        StructField("tr33count", IntegerType(), True),
        StructField("tr33ac", IntegerType(), True),
        StructField("tr33nac", IntegerType(), True),
        StructField("tr33dn", IntegerType(), True),

        StructField("tr34flag", IntegerType(), True),
        StructField("tr34latestdate", IntegerType(), True),
        StructField("tr34count", IntegerType(), True),
        StructField("tr34ac", IntegerType(), True),
        StructField("tr34nac", IntegerType(), True),
        StructField("tr34dn", IntegerType(), True),

        StructField("tr35flag", IntegerType(), True),
        StructField("tr35latestdate", IntegerType(), True),
        StructField("tr35count", IntegerType(), True),
        StructField("tr35ac", IntegerType(), True),
        StructField("tr35nac", IntegerType(), True),
        StructField("tr35dn", IntegerType(), True),

        StructField("tr36flag", IntegerType(), True),
        StructField("tr36latestdate", IntegerType(), True),
        StructField("tr36count", IntegerType(), True),
        StructField("tr36ac", IntegerType(), True),
        StructField("tr36nac", IntegerType(), True),
        StructField("tr36dn", IntegerType(), True),

        StructField("tr37flag", IntegerType(), True),
        StructField("tr37latestdate", IntegerType(), True),
        StructField("tr37count", IntegerType(), True),
        StructField("tr37ac", IntegerType(), True),
        StructField("tr37nac", IntegerType(), True),
        StructField("tr37dn", IntegerType(), True),

        StructField("tr38flag", IntegerType(), True),
        StructField("tr38latestdate", IntegerType(), True),
        StructField("tr38count", IntegerType(), True),
        StructField("tr38ac", IntegerType(), True),
        StructField("tr38nac", IntegerType(), True),
        StructField("tr38dn", IntegerType(), True),

        StructField("tr39flag", IntegerType(), True),
        StructField("tr39latestdate", IntegerType(), True),
        StructField("tr39count", IntegerType(), True),
        StructField("tr39ac", IntegerType(), True),
        StructField("tr39nac", IntegerType(), True),
        StructField("tr39dn", IntegerType(), True),

        StructField("tr40flag", IntegerType(), True),
        StructField("tr40latestdate", IntegerType(), True),
        StructField("tr40count", IntegerType(), True),
        StructField("tr40ac", IntegerType(), True),
        StructField("tr40nac", IntegerType(), True),
        StructField("tr40dn", IntegerType(), True),

        StructField("jr01flag", IntegerType(), True),
        StructField("jr02flag", IntegerType(), True),
        StructField("jr03flag", IntegerType(), True),
        StructField("jr04flag", IntegerType(), True),
        StructField("jr05flag", IntegerType(), True),
        StructField("jr06flag", IntegerType(), True),
        StructField("jr07flag", IntegerType(), True),
        StructField("jr08flag", IntegerType(), True),
        StructField("jr09flag", IntegerType(), True),
        StructField("jr10flag", IntegerType(), True),
        StructField("jr11flag", IntegerType(), True),
        StructField("jr15flag", IntegerType(), True),
        StructField("jr16flag", IntegerType(), True),
        StructField("jr17flag", IntegerType(), True),
        StructField("jr22flag", IntegerType(), True),
        StructField("jr23flag", IntegerType(), True),
        StructField("jr24flag", IntegerType(), True),
        StructField("jr25flag", IntegerType(), True),
        StructField("jr26flag", IntegerType(), True),
        StructField("jr27flag", IntegerType(), True),
        StructField("jr34flag", IntegerType(), True),
        StructField("jr35flag", IntegerType(), True),
        StructField("jr36flag", IntegerType(), True),
        StructField("jr37flag", IntegerType(), True),
        StructField("jr38flag", IntegerType(), True),
        StructField("jr39flag", IntegerType(), True),
        StructField("jr40flag", IntegerType(), True),
        StructField("jr41flag", IntegerType(), True),
        StructField("jr42flag", IntegerType(), True),
        StructField("jr16cat1", IntegerType(), True),    
        StructField("jr16cat2", IntegerType(), True),          
        StructField("jr16cat3", IntegerType(), True),
        StructField("jr16cat4", IntegerType(), True),
        StructField("jr16cat5", IntegerType(), True),
        StructField("jr16cat6", IntegerType(), True),
        StructField("jr16cat7", IntegerType(), True),
        StructField("jr16cat8", IntegerType(), True),

        StructField("ql01achq2", IntegerType(), True),
        StructField("ql01year2", IntegerType(), True),
        StructField("ql02achq3", IntegerType(), True),
        StructField("ql02year3", IntegerType(), True),
        StructField("ql03achq4", IntegerType(), True),
        StructField("ql03year4", IntegerType(), True),
        StructField("ql04achq2", IntegerType(), True),
        StructField("ql04year2", IntegerType(), True),
        StructField("ql05achq3", IntegerType(), True),
        StructField("ql05year3", IntegerType(), True),
        StructField("ql06achq4", IntegerType(), True),
        StructField("ql06year4", IntegerType(), True),
        StructField("ql08achq", IntegerType(), True),
        StructField("ql08year", IntegerType(), True),
        StructField("ql09achq", IntegerType(), True),
        StructField("ql09year", IntegerType(), True),
        StructField("ql10achq4", IntegerType(), True),
        StructField("ql10year4", IntegerType(), True),
        StructField("ql12achq3", IntegerType(), True),
        StructField("ql12year3", IntegerType(), True),
        StructField("ql13achq3", IntegerType(), True),
        StructField("ql13year3", IntegerType(), True),
        StructField("ql14achq3", IntegerType(), True),
        StructField("ql14year3", IntegerType(), True),
        StructField("ql15achq3", IntegerType(), True),
        StructField("ql15year3", IntegerType(), True),
        StructField("ql16achq4", IntegerType(), True),
        StructField("ql16year4", IntegerType(), True),
        StructField("ql17achq4", IntegerType(), True),
        StructField("ql17year4", IntegerType(), True),
        StructField("ql18achq4", IntegerType(), True),
        StructField("ql18year4", IntegerType(), True),
        StructField("ql19achq4", IntegerType(), True),
        StructField("ql19year4", IntegerType(), True),
        StructField("ql20achq4", IntegerType(), True),
        StructField("ql20year4", IntegerType(), True),
        StructField("ql22achq4", IntegerType(), True),
        StructField("ql22year4", IntegerType(), True),
        StructField("ql25achq4", IntegerType(), True),
        StructField("ql25year4", IntegerType(), True),
        StructField("ql26achq4", IntegerType(), True),
        StructField("ql26year4", IntegerType(), True),
        StructField("ql27achq4", IntegerType(), True),
        StructField("ql27year4", IntegerType(), True),
        StructField("ql28achq4", IntegerType(), True),
        StructField("ql28year4", IntegerType(), True),
        StructField("ql32achq3", IntegerType(), True),
        StructField("ql32year3", IntegerType(), True),
        StructField("ql33achq4", IntegerType(), True),
        StructField("ql33year4", IntegerType(), True),
        StructField("ql34achqe", IntegerType(), True),
        StructField("ql34yeare", IntegerType(), True),
        StructField("ql35achq1", IntegerType(), True),
        StructField("ql35year1", IntegerType(), True),
        StructField("ql36achq2", IntegerType(), True),
        StructField("ql36year2", IntegerType(), True),
        StructField("ql37achq", IntegerType(), True),
        StructField("ql37year", IntegerType(), True),
        StructField("ql38achq", IntegerType(), True),
        StructField("ql38year", IntegerType(), True),
        StructField("ql39achq", IntegerType(), True),
        StructField("ql39year", IntegerType(), True),
        StructField("ql41achq2", IntegerType(), True),
        StructField("ql41year2", IntegerType(), True),
        StructField("ql42achq3", IntegerType(), True),
        StructField("ql42year3", IntegerType(), True),
        StructField("ql48achq2", IntegerType(), True),
        StructField("ql48year2", IntegerType(), True),
        StructField("ql49achq3", IntegerType(), True),
        StructField("ql49year3", IntegerType(), True),
        StructField("ql50achq2", IntegerType(), True),
        StructField("ql50year2", IntegerType(), True),
        StructField("ql51achq3", IntegerType(), True),
        StructField("ql51year3", IntegerType(), True),
        StructField("ql52achq2", IntegerType(), True),
        StructField("ql52year2", IntegerType(), True),
        StructField("ql53achq2", IntegerType(), True),
        StructField("ql53year2", IntegerType(), True),
        StructField("ql54achq3", IntegerType(), True),
        StructField("ql54year3", IntegerType(), True),
        StructField("ql55achq2", IntegerType(), True),
        StructField("ql55year2", IntegerType(), True),
        StructField("ql56achq3", IntegerType(), True),
        StructField("ql56year3", IntegerType(), True),
        StructField("ql57achq2", IntegerType(), True),
        StructField("ql57year2", IntegerType(), True),
        StructField("ql58achq3", IntegerType(), True),
        StructField("ql58year3", IntegerType(), True),
        StructField("ql62achq5", IntegerType(), True),
        StructField("ql62year5", IntegerType(), True),
        StructField("ql63achq5", IntegerType(), True),
        StructField("ql63year5", IntegerType(), True),
        StructField("ql64achq5", IntegerType(), True),
        StructField("ql64year5", IntegerType(), True),
        StructField("ql67achq2", IntegerType(), True),
        StructField("ql67year2", IntegerType(), True),
        StructField("ql68achq3", IntegerType(), True),
        StructField("ql68year3", IntegerType(), True),
        StructField("ql72achq2", IntegerType(), True),
        StructField("ql72year2", IntegerType(), True),
        StructField("ql73achq2", IntegerType(), True),
        StructField("ql73year2", IntegerType(), True),
        StructField("ql74achq3", IntegerType(), True),
        StructField("ql74year3", IntegerType(), True),
        StructField("ql76achq2", IntegerType(), True),
        StructField("ql76year2", IntegerType(), True),
        StructField("ql77achq3", IntegerType(), True),
        StructField("ql77year3", IntegerType(), True),
        StructField("ql82achq", IntegerType(), True),
        StructField("ql82year", IntegerType(), True),
        StructField("ql83achq", IntegerType(), True),
        StructField("ql83year", IntegerType(), True),
        StructField("ql84achq", IntegerType(), True),
        StructField("ql84year", IntegerType(), True),
        StructField("ql85achq1", IntegerType(), True),
        StructField("ql85year1", IntegerType(), True),
        StructField("ql86achq2", IntegerType(), True),
        StructField("ql86year2", IntegerType(), True),
        StructField("ql87achq3", IntegerType(), True),
        StructField("ql87year3", IntegerType(), True),
        StructField("ql88achq2", IntegerType(), True),
        StructField("ql88year2", IntegerType(), True),
        StructField("ql89achq3", IntegerType(), True),
        StructField("ql89year3", IntegerType(), True),
        StructField("ql90achq2", IntegerType(), True),
        StructField("ql90year2", IntegerType(), True),
        StructField("ql91achq2", IntegerType(), True),
        StructField("ql91year2", IntegerType(), True),
        StructField("ql92achq1", IntegerType(), True),
        StructField("ql92year1", IntegerType(), True),
        StructField("ql93achq1", IntegerType(), True),
        StructField("ql93year1", IntegerType(), True),
        StructField("ql94achq2", IntegerType(), True),
        StructField("ql94year2", IntegerType(), True),
        StructField("ql95achq3", IntegerType(), True),
        StructField("ql95year3", IntegerType(), True),
        StructField("ql96achq2", IntegerType(), True),
        StructField("ql96year2", IntegerType(), True),
        StructField("ql98achq2", IntegerType(), True),
        StructField("ql98year2", IntegerType(), True),
        StructField("ql99achq2", IntegerType(), True),
        StructField("ql99year2", IntegerType(), True),
        StructField("ql100achq3", IntegerType(), True),
        StructField("ql100year3", IntegerType(), True),
        StructField("ql101achq3", IntegerType(), True),
        StructField("ql101year3", IntegerType(), True),
        StructField("ql102achq5", IntegerType(), True),
        StructField("ql102year5", IntegerType(), True),
        StructField("ql103achq3", IntegerType(), True),
        StructField("ql103year3", IntegerType(), True),
        StructField("ql104achq4", IntegerType(), True),
        StructField("ql104year4", IntegerType(), True),
        StructField("ql105achq4", IntegerType(), True),
        StructField("ql105year4", IntegerType(), True),
        StructField("ql107achq3", IntegerType(), True),
        StructField("ql107year3", IntegerType(), True),
        StructField("ql108achq3", IntegerType(), True),
        StructField("ql108year3", IntegerType(), True),
        StructField("ql109achq4", IntegerType(), True),
        StructField("ql109year4", IntegerType(), True),
        StructField("ql110achq4", IntegerType(), True),
        StructField("ql110year4", IntegerType(), True),
        StructField("ql111achq", IntegerType(), True),
        StructField("ql111year", IntegerType(), True),
        StructField("ql112achq", IntegerType(), True),
        StructField("ql112year", IntegerType(), True),
        StructField("ql113achq", IntegerType(), True),
        StructField("ql113year", IntegerType(), True),
        StructField("ql114achq", IntegerType(), True),
        StructField("ql114year", IntegerType(), True),
        StructField("ql115achq", IntegerType(), True),
        StructField("ql115year", IntegerType(), True),
        StructField("ql116achq", IntegerType(), True),
        StructField("ql116year", IntegerType(), True),
        StructField("ql117achq", IntegerType(), True),
        StructField("ql117year", IntegerType(), True),
        StructField("ql118achq", IntegerType(), True),
        StructField("ql118year", IntegerType(), True),
        StructField("ql119achq", IntegerType(), True),
        StructField("ql119year", IntegerType(), True),
        StructField("ql120achq", IntegerType(), True),
        StructField("ql120year", IntegerType(), True),
        StructField("ql121achq", IntegerType(), True),
        StructField("ql121year", IntegerType(), True),
        StructField("ql122achq", IntegerType(), True),
        StructField("ql122year", IntegerType(), True),
        StructField("ql123achq", IntegerType(), True),
        StructField("ql123year", IntegerType(), True),
        StructField("ql124achq", IntegerType(), True),
        StructField("ql124year", IntegerType(), True),
        StructField("ql125achq", IntegerType(), True),
        StructField("ql125year", IntegerType(), True),
        StructField("ql126achq", IntegerType(), True),
        StructField("ql126year", IntegerType(), True),
        StructField("ql127achq", IntegerType(), True),
        StructField("ql127year", IntegerType(), True),
        StructField("ql128achq", IntegerType(), True),
        StructField("ql128year", IntegerType(), True),
        StructField("ql129achq", IntegerType(), True),
        StructField("ql129year", IntegerType(), True),
        StructField("ql130achq", IntegerType(), True),
        StructField("ql130year", IntegerType(), True),
        StructField("ql131achq", IntegerType(), True),
        StructField("ql131year", IntegerType(), True),
        StructField("ql132achq", IntegerType(), True),
        StructField("ql132year", IntegerType(), True),
        StructField("ql133achq", IntegerType(), True),
        StructField("ql133year", IntegerType(), True),
        StructField("ql134achq", IntegerType(), True),
        StructField("ql134year", IntegerType(), True),
        StructField("ql135achq", IntegerType(), True),
        StructField("ql135year", IntegerType(), True),
        StructField("ql136achq", IntegerType(), True),
        StructField("ql136year", IntegerType(), True),
        StructField("ql137achq", IntegerType(), True),
        StructField("ql137year", IntegerType(), True),
        StructField("ql138achq", IntegerType(), True),
        StructField("ql138year", IntegerType(), True),
        StructField("ql139achq", IntegerType(), True),
        StructField("ql139year", IntegerType(), True),
        StructField("ql140achq", IntegerType(), True),
        StructField("ql140year", IntegerType(), True),
        StructField("ql141achq", IntegerType(), True),
        StructField("ql141year", IntegerType(), True),
        StructField("ql142achq", IntegerType(), True),
        StructField("ql142year", IntegerType(), True),
        StructField("ql143achq", IntegerType(), True),
        StructField("ql143year", IntegerType(), True),
        StructField("ql144achq", IntegerType(), True),
        StructField("ql144year", IntegerType(), True),
        
        StructField("ql301app", IntegerType(), True),
        StructField("ql301year", IntegerType(), True),
        StructField("ql302app", IntegerType(), True),
        StructField("ql302year", IntegerType(), True),
        StructField("ql303app", IntegerType(), True),
        StructField("ql303year", IntegerType(), True),
        StructField("ql304app", IntegerType(), True),
        StructField("ql304year", IntegerType(), True),
        StructField("ql305app", IntegerType(), True),
        StructField("ql305year", IntegerType(), True),
        StructField("ql306app", IntegerType(), True),
        StructField("ql306year", IntegerType(), True),
        StructField("ql307app", IntegerType(), True),
        StructField("ql307year", IntegerType(), True),
        StructField("ql308app", IntegerType(), True),
        StructField("ql308year", IntegerType(), True),
        StructField("ql309app", IntegerType(), True),
        StructField("ql309year", IntegerType(), True),
        StructField("ql310app", IntegerType(), True),
        StructField("ql310year", IntegerType(), True),
        StructField("ql311app", IntegerType(), True),
        StructField("ql311year", IntegerType(), True),
        StructField("ql312app", IntegerType(), True),
        StructField("ql312year", IntegerType(), True),
        StructField("ql313app", IntegerType(), True),
        StructField("ql313year", IntegerType(), True),
    ]
)
