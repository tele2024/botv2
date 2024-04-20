# -*- coding: utf-8 -*-
"""
Created on Sun May 29 11:42:25 2022

@author: soso2
"""
#from bottle import run, post,response,request as bottle_request  

from key import API_KEY
import telebot 
from DBMSv2 import export_to_csv,fetch_data,insert_users,create_tables,connect_to_db
#from telebot import types
from telebot.types import KeyboardButton,ReplyKeyboardMarkup,ReplyKeyboardRemove

#from pydbhelper import DBHelper

bot = telebot.TeleBot(API_KEY)


"""db=DBHelper()
db.setup()"""


def concatFirstSum(listx,x,w,z,listy):
    listy.clear()
    for i in range(len(listx)):
        y=x+" "+listx[i]
        a=w+" "+listx[i]
        b=z+" "+listx[i]
        listy.append(y)
        listy.append(a)
        listy.append(b)
    return listy


@bot.message_handler(commands=['help'])
def send_help(message):
    bot.send_message(chat_id=message.chat.id,text="press /start again")
    print(message)
@bot.message_handler(commands=['return'])
def send_returnMajors(message):
    majorsMarkup=ReplyKeyboardMarkup()
    majorsbtn=[]
    for i in range(len(Majors)):
        majorsbtn=KeyboardButton(Majors[i])
        majorsMarkup.add(majorsbtn)
    bot.send_message(message.chat.id,text="اختر التخصص",reply_markup=majorsMarkup)


@bot.message_handler(commands=['end'])
def endit(message):
    endMarkup=ReplyKeyboardRemove()
    bot.send_message(message.chat.id,text="سعدت بمساعدتكم ",reply_markup=endMarkup)

mainList=["الاطلاع على المواد","كيف اتواصل مع صاحب البوت؟"
          ,"مواقع التواصل الاجتماعي للفريق","قناة التليجرام للمحاضرات"]

types=["متطلب جامعة اجباري","متطلب جامعة اختياري","مواد مساندة اجباري","متطلب كلية اجباري","متطلب تخصص اجباري","متطلب تخصص اختياري"]


uniMandList=["التربية الوطنية","اللغة الانجليزية 1"
         ,"اللغة الانجليزية 2","اللغة العربية 1","مختبر مهارات الحاسوب 1"
         ,"مهارات الحاسوب 1","اللغة العربية 2","علوم عسكرية"]

uniMandListEdt=[]
concatFirstSum(uniMandList, "اسئلة", "ملخص", "كتاب", uniMandListEdt)
uniOptList=["الثقافة الإسلامية","الرياضة والصحة للجميع","الزراعة في الاردن","مباديء علم النفس","مفاهيم اقتصـادية"]
uniOptListedt=[]
concatFirstSum(uniOptList, "اسئلة", "ملخص", "كتاب", uniOptListedt)
supMandList=["الهياكل والرياضيات المنفصلة","الاحتمالات والاحصاء","مبادئ التحليل العددي"]
supMandListEdt=[]
concatFirstSum(supMandList, "اسئلة", "ملخص", "كتاب", supMandListEdt)
collMandList=["التفاضل والتكامل (1)","التفاضل والتكامل (2)","مختبر مهارات حاسوب (2) لطلبة الكليات العلمية","مقدمة الى برمجة الحاسوب","مهارات الحاسوب (2) لطلبة الكليات العلمية","البرمجة الموجهة للكائنات","البرمجة بلغة الجافا","مختبر البرمجة الموجهة للكائنات","مختبر البرمجة بلغة الجافا"]
collMandListEdt=[]
concatFirstSum(collMandList, "اسئلة", "ملخص", "كتاب",collMandListEdt )
Majors=["علم حاسوب","نظم معلومات حاسوبية","هندسة البرمجيات","الرسم الحاسوبي والرسوم المتحركة"]
CISMand_list=["التصميم والتحليل المنطقي الرقمي","مختبر التصميم والتحليل المنطقي الرقمي","مباديء شبكات الحاسوب","نماذج وقواعد البيانات","مختبر نماذج وقواعد البيانات","هياكل البيانات والملفات (1)","مختبر هياكل البيانات والملفات (1)","نظرية الخوارزميات","ادارة نظم قواعد البيانات","مختبر ادارة نظم قواعد البيانات","البرمجة المرئية والواجهات الرسومية","مختبر البرمجة المرئية والواجهات الرسومية","التنقيب عن البيانات","مختبر التنقيب عن البيانات","تحليل و تصميم النظم","تطبيقات وخدمات الويب","مختبر تطبيقات وخدمات الويب","مستودعات البيانات","نظم استرجاع المعلومات","نظم التشغيل وبرمجة الانظمة","ادارة شبكات الحاسوب","امن المعلومات","تراسل الشبكات اللاسلكية","هندسة البرمجيات الحديثة"]
CISMand_listEdt=[]
concatFirstSum(CISMand_list, "اسئلة", "ملخص", "كتاب", CISMand_listEdt)
CSMand_list=["المنطق الرقمي","مختبر المنطق الرقمي","تصميم وادارة قواعد البيانات (1)","مختبر تصميم وادارة قواعد البيانات (1)","تحليل وتصميم الخوارزميات","هياكل البيانات","مختبر هياكل البيانات","البرمجة المرئية للاجهزة الذكية","مختبر البرمجة المرئية للاجهزة الذكية","الذكاء الاصطناعي","النظرية الاحتسابية والاتمتة","امن الحاسوب والشبكات","برمجة تطبيقات الانترنت","مختبر برمجة تطبيقات الانترنت","تحليل و تصميم النظم","تصميم وتنظيم الانظمة المدمجة","شبكات الحاسوب اللاسلكية","مباديء شبكات الحاسوب","تصميم المترجمات ولغات البرمجة","مبادئ نظم التشغيل","معمارية الحاسـوب","هندسة البرمجيات الحديثة"]
CSMand_listEdt=[]
concatFirstSum(CSMand_list, "اسئلة", "ملخص", "كتاب", CSMand_listEdt)
SEMand_list=["قواعد البيانات","مختبر قواعد البيانات","ادوات بناء البرمجيات","الاختبار والتحقق من البرمجيات","البرمجة المرئية لطلبة هندسة البرمجيات","مختبر البرمجة المرئية لطلبة هندسة البرمجيات","المنطق الرقمي","مختبر المنطق الرقمي","تحليل الخوارزميات","تخطيط وادارة مشاريع البرمجيات","تصميم وتنظيم الحاسوب","تصميم وتنفيذ واجهة المستخدم","تطوير تطبيقات الويب","تطوير مكونات البرمجيات","مباديء شبكات الحاسوب","مباديء هندسة البرمجيات","هيكلة البرامج والملفات والبيانات","مختبر هيكلة البرامج والملفات والبيانات","نظرية نظم التشغيل","هندسة البرمجيات الشيئية","هندسة ووصف متطلبات البرمجيات"]
SEMand_listEdt=[]
concatFirstSum(SEMand_list, "اسئلة", "ملخص", "كتاب", SEMand_listEdt)
majorOptlist=["علم الحاسوب","نظم المعلومات الحاسوبية","رسم الحاسوبي والرسوم المتحركة"]
CSOptlist=["الوسائط المتعددة","الحوسبة السحابية","معالجة الصور والرؤيا الرقمية","مواضيع خاصة في علم الحاسوب","الذكاء الاصطناعي المتقدم والتعلم الآلي","ادارة نظم قواعد البيانات"]
CSOptlistEdt=[]
concatFirstSum(CSOptlist, "اسئلة", "ملخص", "كتاب", CSOptlistEdt)
CISOptlist=["تصنيف وتقييم الانماط","انظمة المعـلومات الجغرافية","انظمة التجارة الالكترونية"]
CISOptlistEdt=[]
concatFirstSum(CISOptlist, "اسئلة", "ملخص", "كتاب", CISOptlistEdt)
CGAMandlist=["تحليل وتصميم الخوارزميات","تصميم وادارة قواعد البيانات (1)","مختبر تصميم وادارة قواعد البيانات (1)","الرياضيات للرسم الحاسوبي","مبادئ الرسم الحاسوبي","طرق التلوين","الرسوم المتحركة ثنائية الابعاد","مختبر الرسوم المتحركة ثنائية الابعاد","تطبيقات الحاسوب في الفنون","البرمجة المرئية للرسم الحاسوبي","برمجة تطبيقات الانترنت","مختبر برمجة تطبيقات الانترنت","الرسومات التفاعلية","تصميم القصة المصورة","تصميم النماذج ثلاثية الابعاد","تصميم الشخصيات ثلاثية الابعاد","الوسائط المتعددة","تحريك الشخصيات ثلاثية الابعاد","تفاعل الانسان والحاسب الآلي","تصميم الافلام الرقمية","مختبر تصميم الافلام الرقمية","مبادئ الفنون الجميلة"]
CGAMandlistEdt=[]
concatFirstSum(CGAMandlist, "اسئلة", "ملخص", "كتاب", CGAMandlistEdt)
CGAOptlist=["الحوسبة المتوازية في الرسم الحاسوبي","معالجة الصور والرؤيا الرقمية","تصميم الالعاب","موضوعات خاصة في الرسم الحاسوبي","برمجة الالعاب"]
CGAOptlistEdt=[]
concatFirstSum(CGAOptlist, "اسئلة", "ملخص", "كتاب", CGAOptlistEdt)

#welcoming user and asks him "How can I help?"
@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id,text="اهلًا"+" "+message.from_user.first_name)
    mainMarkup=ReplyKeyboardMarkup()
    mainbtns=[]
    mainbtns2=[]
    for i in range(len(mainList)):
        if(i%2==0 and i<len(mainList)):
            mainbtns=KeyboardButton(mainList[i])
            mainbtns2=KeyboardButton(mainList[i+1])
            mainMarkup.add(mainbtns,mainbtns2)
        else:
            mainbtns=KeyboardButton(mainList[i])
    bot.send_message(message.chat.id,text="كيف يمكنني مساعدتك؟",reply_markup=mainMarkup)
    save_user(message)
    
    
def save_user(message):
     user_id=message.from_user.id
     firstn=message.chat.first_name 
     lastn=message.chat.last_name 
     user=message.chat.username
     conn = connect_to_db()
     create_tables(conn)  
     insert_users(conn,user_id,user, firstn, lastn)
     df = fetch_data(conn)
     export_to_csv(df)
     conn.close()

#checks what he chose
def showTypes(message):
    if (message.text==mainList[0]):
        return True
    else:
        return False   
def communicate(message):
    if (message.text==mainList[1]):
        return True
    else:
        return False
def social(message):
    if (message.text==mainList[2]):
        return True
    else:
        return False
def teleChannel(message):
    if (message.text==mainList[3]):
        return True
    else:
        return False
    
# he chose "الاطلاع على المواد" so the bot shows him "انواع المواد" 
@bot.message_handler(func=showTypes)
def send_types(message):
    typeMarkup=ReplyKeyboardMarkup()
    type_btn=[]
    for i in range(len(types)):
        if(i%2==0 and i<len(types)):
            type_btn=KeyboardButton(types[i])
            type_btn2=KeyboardButton(types[i+1])
            typeMarkup.add(type_btn,type_btn2)
        else:
            type_btn=KeyboardButton(types[i])
    bot.send_message(message.chat.id,text="اختر نوع المادة",reply_markup=typeMarkup)

def uniMand(message):
    if(message.text==types[0]):
        return True 
    else:
        return False
def uniOpt(message):
    if(message.text==types[1]):
        return True 
    else:
        return False
def supMand(message):
    if(message.text==types[2]):
        return True 
    else:
        return False
def collMand(message):
    if(message.text==types[3]):
        return True 
    else:
        return False
def majorMand(message):
    if(message.text==types[4]):
        return True 
    else:
        return False
def majorOpt(message):
    if(message.text==types[5]):
        return True 
    else:
        return False



#مواد جامعة اجباري
@bot.message_handler(func=uniMand)
def send_uniMand(message):
    uniMandMarkup=ReplyKeyboardMarkup()
    uniMandbtn=[]
    for i in range(len(uniMandList)):
        if(i%2==0 and i<len(uniMandList)):
            uniMandbtn=KeyboardButton(uniMandList[i])
            uniMandbtn2=KeyboardButton(uniMandList[i+1])
            uniMandMarkup.add(uniMandbtn,uniMandbtn2)
        else:
            uniMandbtn=KeyboardButton(uniMandList[i])
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=uniMandMarkup)
def edu(message):
    if(message.text==uniMandList[0]):
        return True
    else:
        return False
def eng1(message):
    if(message.text==uniMandList[1]):
        return True
    else:
        return False
def eng2(message):
    if(message.text==uniMandList[2]):
        return True
    else:
        return False  
def arab1(message):
    if(message.text==uniMandList[3]):
        return True
    else:
        return False 
def labComp1(message):
    if(message.text==uniMandList[4]):
        return True
    else:
        return False  
def comp1(message):
    if(message.text==uniMandList[5]):
        return True
    else:
        return False
def arab2(message):
    if(message.text==uniMandList[6]):
        return True
    else:
        return False
def mili(message):
    if(message.text==uniMandList[7]):
        return True
    else:
        return False
#edu

@bot.message_handler(func=edu)
def send_edu(message):
    eduMarkup=ReplyKeyboardMarkup()
    edubtn=[]
    for i in range(3):
        edubtn=KeyboardButton(uniMandListEdt[i])
        eduMarkup.add(edubtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=eduMarkup)
def eduQuest(message):
    if (message.text==uniMandListEdt[0]):
        return True
    else:
        return False
def eduSummary(message):
    if (message.text==uniMandListEdt[1]):
        return True
    else:
        return False
def eduBook(message):
    if (message.text==uniMandListEdt[2]):
        return True
    else:
        return False
@bot.message_handler(func=eduQuest)
def send_eduQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1cQAignxNxyO4wxcbuOhrwOCCTr7auvbd?usp=sharing")

@bot.message_handler(func=eduSummary)
def send_eduSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1xy3y4IeFrdMqYpz_16Tp0zsGmcpLGQ7W?usp=sharing")

@bot.message_handler(func=eduBook)
def send_eduBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1IyGaFBAVGYZhT3l65Xfm44w43i-M0GYb?usp=sharing")

#eng1

@bot.message_handler(func=eng1)
def send_eng1(message):
    eng1Markup=ReplyKeyboardMarkup()
    eng1btn=[]
    for i in range(3,5):
        eng1btn=KeyboardButton(uniMandListEdt[i])
        eng1Markup.add(eng1btn)
    eng1btn2=KeyboardButton("سلايدات اللغة الانجليزية 1")
    eng1Markup.add(eng1btn2)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=eng1Markup)
def eng1Quest(message):
    if (message.text==uniMandListEdt[3]):
        return True
    else:
        return False
def eng1Summary(message):
    if (message.text==uniMandListEdt[4]):
        return True
    else:
        return False
def eng1Book(message):
    if (message.text=="سلايدات اللغة الانجليزية 1"):
        return True
    else:
        return False

@bot.message_handler(func=eng1Quest)
def send_eng1Quest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1w5FE7aWh9zWHzRdIgm_SshZuhTM1AAGs?usp=sharing")

@bot.message_handler(func=eng1Summary)
def send_eng1Summary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1am6TwEJQJWIYS7sXmyGClVO-h1Tr2S9O?usp=sharing")

@bot.message_handler(func=eng1Book)
def send_eng1Book(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1m4zGtpbtvf-VKomKcna7dH9pgRy1FEoE?usp=sharing")
    
    
#eng2

@bot.message_handler(func=eng2)
def send_eng2(message):
    eng2Markup=ReplyKeyboardMarkup()
    eng2btn=[]
    for i in range(6,9):
        eng2btn=KeyboardButton(uniMandListEdt[i])
        eng2Markup.add(eng2btn)
    eng2btn2=KeyboardButton("كويزات اللغة الانجليزية 2")
    eng2Markup.add(eng2btn2)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=eng2Markup)
def eng2Quest(message):
    if (message.text==uniMandListEdt[6]):
        return True
    else:
        return False
def eng2Summary(message):
    if (message.text==uniMandListEdt[7]):
        return True
    else:
        return False
def eng2Book(message):
    if (message.text==uniMandListEdt[8]):
        return True
    else:
        return False
def eng2Quizes(message):
    if(message.text=="كويزات اللغة الانجليزية 2"):
        return True
    else:
        return False
    

@bot.message_handler(func=eng2Quest)
def send_eng2Quest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Kl-C8B0jF2a1deYBszM6H9CUc1DqBkPK?usp=sharing")

@bot.message_handler(func=eng2Summary)
def send_eng2Summary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/18Ep3pRc_EbNGfBhB8IQhkZv2VT-_MFdr?usp=sharing")

@bot.message_handler(func=eng2Book)
def send_eng2Book(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1ni_KwL_dgnqKGdSGuN3-MCfFCGI9AaRO?usp=sharing")


@bot.message_handler(func=eng2Quizes)
def send_eng2Quizes(message):
    bot.send_message(message.chat.id,text="الوحدة الاولى +الثانية"+"\n"+"https://drive.google.com/drive/folders/1jNkT2uNadwKjL6AWRollmt-5RGzuP5Y8?usp=sharing")
    bot.send_message(message.chat.id,text="الوحدة الثالثة +الرابعة"+"\n"+"https://drive.google.com/drive/folders/18fxRS0ZwFq7QcXGy20QnHzjWgWPStALK?usp=sharing")

#arab1
    
@bot.message_handler(func=arab1)
def send_arab1(message):
    arab1Markup=ReplyKeyboardMarkup()
    arab1btn=[]
    for i in range(9,12):
        arab1btn=KeyboardButton(uniMandListEdt[i])
        arab1Markup.add(arab1btn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=arab1Markup)
def arab1Quest(message):
    if (message.text==uniMandListEdt[9]):
        return True
    else:
        return False
def arab1Summary(message):
    if (message.text==uniMandListEdt[10]):
        return True
    else:
        return False
def arab1Book(message):
    if (message.text==uniMandListEdt[11]):
        return True
    else:
        return False

    

@bot.message_handler(func=arab1Quest)
def send_arab1Quest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1mC_G1M5OtiShtYrj7djgXXmpjxqacAOF?usp=drive_link")

@bot.message_handler(func=arab1Summary)
def send_arab1Summary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1GaxvkVeKCQNoB25W0zHWQReMuclbohPL?usp=drive_link")

@bot.message_handler(func=arab1Book)
def send_arab1Book(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1mRxtOWly7xSCG-1Q2jyfC2czw2_-C8gi?usp=drive_link")

#labcomp1
@bot.message_handler(func=labComp1)
def send_labComp1(message):
    labComp1Markup=ReplyKeyboardMarkup()
    labComp1btn=[]
    for i in range(12,15):
        if(i==13):
            continue
        labComp1btn=KeyboardButton(uniMandListEdt[i])
        labComp1Markup.add(labComp1btn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=labComp1Markup)
def labComp1Quest(message):
    if (message.text==uniMandListEdt[12]):
        return True
    else:
        return False
    
def labComp1Book(message):
    if (message.text==uniMandListEdt[14]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=labComp1Quest)
def send_labComp1Quest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1S20ZphHHCULLFZbJqJ0vQq3tWNDAttf-?usp=sharing")

@bot.message_handler(func=labComp1Book)
def send_labComp1Book(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1boxJm_w1pPgRJ6ufnmvRw9kd_TA7z3NQ?usp=sharing")
    
#comp1
@bot.message_handler(func=comp1)
def send_comp1(message):
    comp1Markup=ReplyKeyboardMarkup()
    comp1btn=[]
    for i in range(15,18):
        if(i==16):
            continue
        comp1btn=KeyboardButton(uniMandListEdt[i])
        comp1Markup.add(comp1btn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=comp1Markup)
def comp1Quest(message):
    if (message.text==uniMandListEdt[15]):
        return True
    else:
        return False

def comp1Book(message):
    if (message.text==uniMandListEdt[17]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=comp1Quest)
def send_comp1Quest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1aJxiA44IVW8T9AumNbFryaYSQN1qqmLA?usp=sharing")


@bot.message_handler(func=comp1Book)
def send_comp1Book(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1v9PDTOs_-8UMHs8ZhFf6Krj6D-hQG-Ix?usp=sharing")
    


#arab2
@bot.message_handler(func=arab2)
def send_arab2(message):
    arab2Markup=ReplyKeyboardMarkup()
    arab2btn=[]
    for i in range(18,21):
        arab2btn=KeyboardButton(uniMandListEdt[i])
        arab2Markup.add(arab2btn)
    arab2btn2=KeyboardButton("اختبار محاكي لمادة الميد اللغة العربية 2")
    arab2Markup.add(arab2btn2)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=arab2Markup)
def arab2Quest(message):
    if (message.text==uniMandListEdt[18]):
        return True
    else:
        return False
def arab2Summary(message):
    if (message.text==uniMandListEdt[19]):
        return True
    else:
        return False
def arab2Book(message):
    if (message.text==uniMandListEdt[20]):
        return True
    else:
        return False
def arab2mid(message):
    if (message.text=="اختبار محاكي لمادة الميد اللغة العربية 2"):
        return True
    else:
        return False
    
    

@bot.message_handler(func=arab2Quest)
def send_arab2Quest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1O2kDhjAG386Le5wTEwW8DWv4XtC6vO_r?usp=sharing")

@bot.message_handler(func=arab2Summary)
def send_arab2Summary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1RAhyW-FtIUSeBZxcfLvP8rkw1tCMrm-I?usp=sharing")

@bot.message_handler(func=arab2Book)
def send_arab2Book(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1rIAvO7cjGTeNy9vgF8MMU32vZ-Fw8_2P?usp=sharing")
    
@bot.message_handler(func=arab2mid)
def send_arab2mid(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1pYtB2w8VzTOj4PgZl_M6W9T4rPjvj2LR?usp=sharing")

#mili
@bot.message_handler(func=mili)
def send_mili(message):
    miliMarkup=ReplyKeyboardMarkup()
    milibtn=[]
    for i in range(21,24):
        milibtn=KeyboardButton(uniMandListEdt[i])
        miliMarkup.add(milibtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=miliMarkup)
def miliQuest(message):
    if (message.text==uniMandListEdt[21]):
        return True
    else:
        return False
def miliSummary(message):
    if (message.text==uniMandListEdt[22]):
        return True
    else:
        return False
def miliBook(message):
    if (message.text==uniMandListEdt[23]):
        return True
    else:
        return False
    

@bot.message_handler(func=miliQuest)
def send_miliQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1BetfuZrF-1BmYmfKF0rGzGBffQYHkLxQ?usp=sharing")

@bot.message_handler(func=arab1Summary)
def send_miliSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1fZLHy-jKdWhMfK0pielIJwEatr1HFVTb?usp=sharing")

@bot.message_handler(func=arab1Book)
def send_miliBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1VjRTYasFP6aSZRJ9zVkMPdfBdVbTmlxo?usp=sharing")
       
    
@bot.message_handler(func=uniOpt)
def send_uniOpt(message):
    uniOptMarkup=ReplyKeyboardMarkup()
    uniOptbtn=[]
    for i in range(len(uniOptList)):
            uniOptbtn=KeyboardButton(uniOptList[i])
            uniOptMarkup.add(uniOptbtn)
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=uniOptMarkup)
def eduIslam(message):
    if(message.text==uniOptList[0]):
        return True
    else:
        return False
def sport(message):
    if(message.text==uniOptList[1]):
        return True
    else:
        return False
def plant(message):
    if(message.text==uniOptList[2]):
        return True
    else:
        return False  
def psychalogy(message):
    if(message.text==uniOptList[3]):
        return True
    else:
        return False 
def econo(message):
    if(message.text==uniOptList[4]):
        return True
    else:
        return False  



@bot.message_handler(func=eduIslam)
def send_eduIslam(message):
    eduIslamMarkup=ReplyKeyboardMarkup()
    eduIslambtn=[]
    for i in range(3):
        if(i==1):
            continue
        eduIslambtn=KeyboardButton(uniOptListedt[i])
        eduIslamMarkup.add(eduIslambtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=eduIslamMarkup)
def eduIslamQuest(message):
    if (message.text==uniOptListedt[0]):
        return True
    else:
        return False

def eduIslamBook(message):
    if (message.text==uniOptListedt[2]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=eduIslamQuest)
def send_eduIslamQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Z_XSrobUDhnhAsEmOz5t-K5Wmme8VRYX?usp=sharing")



@bot.message_handler(func=eduIslamBook)
def send_eduIslamBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1IbQi6OnODVj2QeDwBT9_LYdaRMd-gdol?usp=sharing")


@bot.message_handler(func=sport)
def send_sport(message):
    sportMarkup=ReplyKeyboardMarkup()
    sportbtn=[]
    for i in range(3,6):
        if(i==4):
            continue
        sportbtn=KeyboardButton(uniOptListedt[i])
        sportMarkup.add(sportbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=sportMarkup)
def sportQuest(message):
    if (message.text==uniOptListedt[3]):
        return True
    else:
        return False

def sportBook(message):
    if (message.text==uniOptListedt[5]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=sportQuest)
def send_sportQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1IfFxlEVZaIERGJ_Qe4K1FkXdR7qY2tIg?usp=sharing")



@bot.message_handler(func=sportBook)
def send_sportBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1BdgXalf-y3Xf0pvkGLME3fqzmIeIjxl0?usp=sharing")


@bot.message_handler(func=plant)
def send_plant(message):
    plantMarkup=ReplyKeyboardMarkup(resize_keyboard=True)
    plantbtn=[]
    for i in range(6,9):
        if(i==7 or i==8):
            continue
        plantbtn=KeyboardButton(uniOptListedt[i])
        plantMarkup.add(plantbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=plantMarkup)
def plantQuest(message):
    if (message.text==uniOptListedt[6]):
        return True
    else:
        return False


@bot.message_handler(func=plantQuest)
def send_plantQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1AfsE5seHE0DSoRgGJTMaHoquLZesogbH?usp=sharing")


@bot.message_handler(func=psychalogy)
def send_psychalogy(message):
    psychalogyMarkup=ReplyKeyboardMarkup()
    psychalogybtn=[]
    for i in range(9,12):
        psychalogybtn=KeyboardButton(uniOptListedt[i])
        psychalogyMarkup.add(psychalogybtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=psychalogyMarkup)
def psychalogyQuest(message):
    if (message.text==uniOptListedt[9]):
        return True
    else:
        return False
def psychalogySummary(message):
    if (message.text==uniOptListedt[10]):
        return True
    else:
        return False
def psychalogyBook(message):
    if (message.text==uniOptListedt[11]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=psychalogyQuest)
def send_psychalogyQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1l4MmmYEn0i4YPhUPTV-jyKsFZxizeTSn?usp=sharing")

@bot.message_handler(func=psychalogySummary)
def send_psychalogySummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1KILdXV7bljXDtruAsCt_HiVstrXOrCbv?usp=sharing")

@bot.message_handler(func=psychalogyBook)
def send_psychalogyBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1HchT64c--vxQ_3tStNt6pZ8m1RHyLQ-1?usp=sharing")

@bot.message_handler(func=econo)
def send_econo(message):
    econoMarkup=ReplyKeyboardMarkup()
    econobtn=[]
    for i in range(12,15):
        if(i==13):
            continue
        econobtn=KeyboardButton(uniOptListedt[i])
        econoMarkup.add(econobtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=econoMarkup)
def econoQuest(message):
    if (message.text==uniOptListedt[12]):
        return True
    else:
        return False
    
def econoBook(message):
    if (message.text==uniOptListedt[14]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=econoQuest)
def send_econoQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1J3wiR9KYKGl9bH2BQrOnEV1eadTfvwp8?usp=sharing")



@bot.message_handler(func=econoBook)
def send_econoBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1QTF88NAs_XftS8R7wFI56gIUdDCkZYto?usp=sharing")



@bot.message_handler(func=supMand)
def send_supMand(message):
    supMandMarkup=ReplyKeyboardMarkup()
    supMandbtn=[]
    for i in range(len(supMandList)):
            supMandbtn=KeyboardButton(supMandList[i])
            supMandMarkup.add(supMandbtn)
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=supMandMarkup)
def dis(message):
    if(message.text==supMandList[0]):
        return True
    else:
        return False
def prob(message):
    if(message.text==supMandList[1]):
        return True
    else:
        return False
def numAna(message):
    if(message.text==supMandList[2]):
        return True
    else:
        return False  

@bot.message_handler(func=dis)
def send_dis(message):
    disMarkup=ReplyKeyboardMarkup()
    disbtn=[]
    for i in range(3):
        disbtn=KeyboardButton(supMandListEdt[i])
        disMarkup.add(disbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=disMarkup)
def disQuest(message):
    if (message.text==supMandListEdt[0]):
        return True
    else:
        return False
def disSummary(message):
    if (message.text==supMandListEdt[1]):
        return True
    else:
        return False
def disBook(message):
    if (message.text==supMandListEdt[2]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=disQuest)
def send_disQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1wBVvHzNl-lS4dex5hChehaKl-_mObXGu?usp=sharing")
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1H8zgeHP2Eovk6SVEuNOx9d1zMFeWE-ES?usp=sharing")
    
@bot.message_handler(func=disSummary)
def send_disSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/19Bz416a_U1jLhSDNi3up8XPZG6dc6TXi?usp=sharing")

@bot.message_handler(func=disBook)
def send_disBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1VaUqvsZSSNHcFe6Ddw-jj_VGSPg8KXxk?usp=sharing")




@bot.message_handler(func=prob)
def send_prob(message):
    probMarkup=ReplyKeyboardMarkup()
    probbtn=[]
    for i in range(3,6):
        probbtn=KeyboardButton(supMandListEdt[i])
        probMarkup.add(probbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=probMarkup)
def probQuest(message):
    if (message.text==supMandListEdt[3]):
        return True
    else:
        return False
def probSummary(message):
    if (message.text==supMandListEdt[4]):
        return True
    else:
        return False
def probBook(message):
    if (message.text==supMandListEdt[5]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=probQuest)
def send_probQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/10YEFFUXYX92op79Ej4pqQqEaagKTf0lM?usp=sharing")

@bot.message_handler(func=probSummary)
def send_probSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Krlp3I_CFbC-1zSpkKVI6V8rraV0q_1S?usp=sharing")

@bot.message_handler(func=probBook)
def send_probBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/13fAnyhtBzJw1PT3rl9XTTatCV3na9SAV?usp=sharing")

supMandListEdt[8]="سلايدات مبادئ التحليل العددي"

@bot.message_handler(func=numAna)
def send_numAna(message):
    numAnaMarkup=ReplyKeyboardMarkup()
    numAnabtn=[]
    for i in range(6,9):
        numAnabtn=KeyboardButton(supMandListEdt[i])
        numAnaMarkup.add(numAnabtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=numAnaMarkup)
def numAnaQuest(message):
    if (message.text==supMandListEdt[6]):
        return True
    else:
        return False
def numAnaSummary(message):
    if (message.text==supMandListEdt[7]):
        return True
    else:
        return False
def numAnaBook(message):
    if (message.text==supMandListEdt[8]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=numAnaQuest)
def send_numAnaQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1YRByWM3A48hmXgDAiV1Qpy_91G5b_boI?usp=sharing")

@bot.message_handler(func=numAnaSummary)
def send_numAnaSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1uic0oHHMlKsPqghw_Ah92oa-WgXzjUEx?usp=sharing")

@bot.message_handler(func=numAnaBook)
def send_numAnaBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/14lDpxS7NCLKUTj_3Hd1K4ET57y07Vsyr?usp=sharing")

#متطلب كلية اجباري

@bot.message_handler(func=collMand)
def send_collMand(message):
    collMandMarkup=ReplyKeyboardMarkup()
    collMandbtn=[]
    for i in range(len(collMandList)):
        if(i%2==0 and i<len(collMandList)-1):
            collMandbtn=KeyboardButton(collMandList[i])
            collMandbtn2=KeyboardButton(collMandList[i+1])
            collMandMarkup.add(collMandbtn,collMandbtn2)
        else:
            collMandbtn=KeyboardButton(collMandList[i])
    collMandMarkup.add(KeyboardButton(collMandList[8]))
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=collMandMarkup)
def calc1(message):
    if(message.text==collMandList[0]):
        return True
    else:
        return False
def calc2(message):
    if(message.text==collMandList[1]):
        return True
    else:
        return False
def labComp2(message):
    if(message.text==collMandList[2]):
        return True
    else:
        return False  
def intro(message):
    if(message.text==collMandList[3]):
        return True
    else:
        return False 
def comp2(message):
    if(message.text==collMandList[4]):
        return True
    else:
        return False  
def objec(message):
    if(message.text==collMandList[5]):
        return True
    else:
        return False
def java(message):
    if(message.text==collMandList[6]):
        return True
    else:
        return False
def labObject(message):
    if(message.text==collMandList[7]):
        return True
    else:
        return False
def labJava(message):
    if(message.text==collMandList[8]):
        return True
    else:
        return False


#calc1

@bot.message_handler(func=calc1)
def send_calc1(message):
    calc1Markup=ReplyKeyboardMarkup()
    calc1btn=[]
    for i in range(3):
        calc1btn=KeyboardButton(collMandListEdt[i])
        calc1Markup.add(calc1btn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=calc1Markup)
def calc1Quest(message):
    if (message.text==collMandListEdt[0]):
        return True
    else:
        return False
def calc1Summary(message):
    if (message.text==collMandListEdt[1]):
        return True
    else:
        return False
def calc1Book(message):
    if (message.text==collMandListEdt[2]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=calc1Quest)
def send_calc1Quest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1yfO-6OExZmLYrdKKDwHn8yTpxuLQm_Gf?usp=sharing")

@bot.message_handler(func=calc1Summary)
def send_calc1Summary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1hP9FsupfPD0JIMm9fRYWQE8Sbqjk1mcu?usp=sharing")

@bot.message_handler(func=calc1Book)
def send_calc1Book(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Qta_Gj3fnH4pfV-S21az-VQYfXsujYeC?usp=sharing")


#calc2

@bot.message_handler(func=calc2)
def send_calc2(message):
    calc2Markup=ReplyKeyboardMarkup()
    calc2btn=[]
    for i in range(3,6):
        calc2btn=KeyboardButton(collMandListEdt[i])
        calc2Markup.add(calc2btn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=calc2Markup)
def calc2Quest(message):
    if (message.text==collMandListEdt[3]):
        return True
    else:
        return False
def calc2Summary(message):
    if (message.text==collMandListEdt[4]):
        return True
    else:
        return False
def calc2Book(message):
    if (message.text==collMandListEdt[5]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=calc2Quest)
def send_calc2Quest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1VfOFUKXbNMTuPV-6it0vQm6iID7B7yHE?usp=sharing")

@bot.message_handler(func=calc2Summary)
def send_calc2Summary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1vy6kH2G-DfVwJlRpqM3pQgVkHB0IyzEL?usp=sharing")

@bot.message_handler(func=calc2Book)
def send_calc2Book(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1yNY0w9NhNRVTPvqZOvGeDIW2KG7UuHRp?usp=sharing")


#labComp2

@bot.message_handler(func=labComp2)
def send_labComp2(message):
    labComp2Markup=ReplyKeyboardMarkup()
    labComp2btn=KeyboardButton("سلايدات مختبر مهارات حاسوب (2) لطلبة الكليات العلمية")
    labComp2Markup.add(labComp2btn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=labComp2Markup)

def labComp2slides(message):
    if (message.text=="سلايدات مختبر مهارات حاسوب (2) لطلبة الكليات العلمية"):
        return True
    else:
        return False

@bot.message_handler(func=labComp2slides)
def send_labComp2slides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1JZXK7GfjxNa9IQqpdIgknxyQteSsyCmD?usp=sharing")

#introduction to Computer programming
collMandListEdt[10]="سلايدات وملخص مقدمة الى برمجة الحاسوب"
@bot.message_handler(func=intro)
def send_intro(message):
    introMarkup=ReplyKeyboardMarkup()
    introbtn=[]
    for i in range(9,12):
        introbtn=KeyboardButton(collMandListEdt[i])
        introMarkup.add(introbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=introMarkup)
def introQuest(message):
    if (message.text==collMandListEdt[9]):
        return True
    else:
        return False
def introSummary(message):
    if (message.text==collMandListEdt[10]):
        return True
    else:
        return False
def introBook(message):
    if (message.text==collMandListEdt[11]):
        return True
    else:
        return False


@bot.message_handler(func=introQuest)
def send_introQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1w36sxUe5YmVDcUHoPuVaRKDtwztU8hi3?usp=drive_link")
@bot.message_handler(func=introSummary)
def send_introSummary(message):
    bot.send_message(message.chat.id,text="سلايدات : https://drive.google.com/drive/folders/1W9bpOOmRzjizT4Aq7UgF_qIA1Yb8nDeb?usp=drive_link")
    bot.send_message(message.chat.id,text="ملخص : https://drive.google.com/drive/folders/1PqjJH0A-jb2JRfNtx0P4ZvSxObpQH0vL?usp=drive_link")
@bot.message_handler(func=introBook)
def send_introBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1lU15xCDJVvV_QXzT_A_MsXAEYIjb2Run?usp=drive_link")

#comp2

collMandListEdt[13]="سلايدات مهارات الحاسوب (2) لطلبة الكليات العلمية"
@bot.message_handler(func=comp2)
def send_comp2(message):
    comp2Markup=ReplyKeyboardMarkup()
    comp2btn=[]
    for i in range(12,15):
        comp2btn=KeyboardButton(collMandListEdt[i])
        comp2Markup.add(comp2btn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=comp2Markup)
def comp2Quest(message):
    if (message.text==collMandListEdt[12]):
        return True
    else:
        return False
def comp2Slides(message):
    if (message.text==collMandListEdt[13]):
        return True
    else:
        return False
def comp2Book(message):
    if (message.text==collMandListEdt[14]):
        return True
    else:
        return False
    
@bot.message_handler(func=comp2Quest)
def send_comp2Quest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1mwz7UE4vRzorfFlHS5sa4isd-oD4dzMG?usp=sharing")

@bot.message_handler(func=comp2Slides)
def send_comp2Slides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1voqW_AtC5KPm1zjpwO47JkFA9wqlKkL1?usp=sharing")

@bot.message_handler(func=comp2Book)
def send_comp2Book(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Opy4WC6SUWdfA9qEf4h9Xzgs3YCQvJe4?usp=sharing")

#Object

@bot.message_handler(func=objec)
def send_objec(message):
    objecMarkup=ReplyKeyboardMarkup()
    objecbtn=[]
    for i in range(15,18):
        objecbtn=KeyboardButton(collMandListEdt[i])
        objecMarkup.add(objecbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=objecMarkup)
def objecQuest(message):
    if (message.text==collMandListEdt[15]):
        return True
    else:
        return False
def objecSummary(message):
    if (message.text==collMandListEdt[16]):
        return True
    else:
        return False
def objecBook(message):
    if (message.text==collMandListEdt[17]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=objecQuest)
def send_objecQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1UKMA5TbJLp57-TsNbo3oSYQuxHYepaOT?usp=drive_link")

@bot.message_handler(func=objecSummary)
def send_objecSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/16Bwnetoq6FZUC2ZiAV74EnTW3w156L1S?usp=sharing")

@bot.message_handler(func=objecBook)
def send_objecBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Hpd4h2BajJ4YesGXW1aGZEzdpkN3js3L?usp=drive_link")

#java

@bot.message_handler(func=java)
def send_java(message):
    javaMarkup=ReplyKeyboardMarkup()
    javabtn=[]
    for i in range(18,21):
        javabtn=KeyboardButton(collMandListEdt[i])
        javaMarkup.add(javabtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=javaMarkup)
def javaQuest(message):
    if (message.text==collMandListEdt[18]):
        return True
    else:
        return False
def javaSummary(message):
    if (message.text==collMandListEdt[19]):
        return True
    else:
        return False
def javaBook(message):
    if (message.text==collMandListEdt[20]):
        return True
    else:
        return False
    
    

@bot.message_handler(func=javaQuest)
def send_javaQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1WIPsMjGAIDRVCLyWs2dTSWsNVa5I9A0t?usp=drive_link")

@bot.message_handler(func=javaSummary)
def send_javaSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/18Q2b8d3FGivr4SOyk9MaRrtxqq4Zoqan?usp=drive_link")

@bot.message_handler(func=javaBook)
def send_javaBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1L-IMvPLQdsKueGtzE5EmbeoCa8LAmXAp?usp=drive_link")

#labObject
collMandListEdt[22]="سلايدات مختبر البرمجة الموجهة للكائنات"

@bot.message_handler(func=labObject)
def send_labObject(message):
    labObjectMarkup=ReplyKeyboardMarkup()
    labObjectbtn=[]
    for i in range(21,23):
        labObjectbtn=KeyboardButton(collMandListEdt[i])
        labObjectMarkup.add(labObjectbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=labObjectMarkup)
def labObjectQuest(message):
    if (message.text==collMandListEdt[21]):
        return True
    else:
        return False
def labObjectSlides(message):
    if (message.text==collMandListEdt[22]):
        return True
    else:
        return False

@bot.message_handler(func=labObjectQuest)
def send_labObjectQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1g9brwLbxuVQaUiyT5xHdGTHyjZWBy1CP?usp=sharing")

@bot.message_handler(func=labObjectSlides)
def send_labObjectSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1uUJ4LyE0Uv4cYqjRF0AmXzD5pdIAw7cb?usp=sharing")

#متطلب تخصص اجباري 

@bot.message_handler(func=majorMand)
def send_majors(message):
    majorsMarkup=ReplyKeyboardMarkup()
    majorsbtn=[]
    for i in range(len(Majors)):
        majorsbtn=KeyboardButton(Majors[i])
        majorsMarkup.add(majorsbtn)
    bot.send_message(message.chat.id,text="اختر التخصص",reply_markup=majorsMarkup)
    
def CS(message):
    if(message.text==Majors[0]):
        return True
    else:
        return False
def CIS(message):
    if(message.text==Majors[1]):
        return True
    else:
        return False
def SE(message):
    if(message.text==Majors[2]):
        return True
    else:
        return False
def CGA(message):
    if(message.text==Majors[3]):
        return True
    else:
        return False

#######################
#####CS Mandatory######
#######################

@bot.message_handler(func=CS)
def send_CSmand(message):
    CSMandMarkup=ReplyKeyboardMarkup()
    CSMandbtn=[]
    for i in range(len(CSMand_list)):
        if(i%2==0 and i<len(CSMand_list)):
            CSMandbtn=KeyboardButton(CSMand_list[i])
            CSMandbtn2=KeyboardButton(CSMand_list[i+1])
            CSMandMarkup.add(CSMandbtn,CSMandbtn2)
        else:
            CSMandbtn=KeyboardButton(CSMand_list[i])
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=CSMandMarkup)
        
def logicCS(message):
    if(message.text==CSMand_list[0]  or message.text==SEMand_list[6]):
        return True
    else:
        return False
def logicCSLab(message):
    if (message.text==CSMand_list[1] or message.text==SEMand_list[7]):
        return True
    else:
        return False
def dbCS(message):
    if(message.text==CSMand_list[2]):
        return True
    else:
        return False
def dbCSLab(message):
    if(message.text==CSMand_list[3]):
        return True
    else:
        return False
def algoCS(message):
    if(message.text==CSMand_list[4]):
        return True
    else:
        return False
def DSCS(message):
    if(message.text==CSMand_list[5]):
        return True
    else:
        return False
def DSCSlab(message):
    if(message.text==CSMand_list[6]):
        return True
    else:
        return False

def AndCS(message):
    if(message.text==CSMand_list[7]):
        return True
    else:
        return False
def AndCSlab(message):
    if(message.text==CSMand_list[8]):
        return True
    else:
        return False 
def AICS(message):
    if(message.text==CSMand_list[9]):
        return True
    else:
        return False
def AutoCS(message):
    if(message.text==CSMand_list[10]):
        return True
    else:
        return False
def SecCS(message):
    if(message.text==CSMand_list[11]):
        return True
    else:
        return False
def webCS(message):
    if(message.text==CSMand_list[12]):
        return True
    else:
        return False
def webCSlab(message):
    if(message.text==CSMand_list[13]):
        return True
    else:
        return False
def SACS(message):
    if(message.text==CSMand_list[14]):
        return True
    else:
        return False
def EmbCS(message):
    if(message.text==CSMand_list[15]):
        return True
    else:
        return False
def WirlessCS(message):
    if(message.text==CSMand_list[16]):
        return True
    else:
        return False
def netCS(message):
    if(message.text==CSMand_list[17]):
        return True
    else:
        return False
def CompilerCS(message):
    if(message.text==CSMand_list[18]):
        return True
    else:
        return False
def OSCS(message):
    if(message.text==CSMand_list[19]):
        return True
    else:
        return False
def ArchCS(message):
    if(message.text==CSMand_list[20]):
        return True
    else:
        return False
def MSECS(message):
    if(message.text==CSMand_list[21]):
        return True
    else:
        return False












#logic CS and SE
@bot.message_handler(func=logicCS)
def send_logicCS(message):
    logicCSMarkup=ReplyKeyboardMarkup()
    logicCSbtn=[]
    for i in range(3):
        logicCSbtn=KeyboardButton(CSMand_listEdt[i])
        logicCSMarkup.add(logicCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=logicCSMarkup)
def logicCSQuest(message):
    if(message.text==CSMand_listEdt[0]):
        return True
    else:
        return False
def logicCSSummary(message):
    if(message.text==CSMand_listEdt[1]):
        return True
    else:
        return False
def logicCSBook(message):
    if(message.text==CSMand_listEdt[2]):
        return True
    else:
        return False
@bot.message_handler(func=logicCSQuest)
def send_logicCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1SqSqbBqUho84Tay84GBLYNwTuxoVsgcT?usp=sharing")
@bot.message_handler(func=logicCSSummary)
def send_logicCSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1cp0cEaR5J-_IiD5tYagHb-QuS5kNsVtv?usp=sharing")
@bot.message_handler(func=logicCSBook)
def send_logicCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1tQ2QFiNMznTkqlAosViQpcmIMMp8W9-h?usp=sharing")

#logic CS Lab
@bot.message_handler(func=logicCSLab)
def send_logicCSLab(message):
    logicCSLabMarkup=ReplyKeyboardMarkup()
    logicCSLabbtn=[]
    for i in range(3,6):
        if(i==4):
            continue
        logicCSLabbtn=KeyboardButton(CSMand_listEdt[i])
        logicCSLabMarkup.add(logicCSLabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=logicCSLabMarkup)
def logicCSLabQuest(message):
    if(message.text==CSMand_listEdt[3]):
        return True
    else:
        return False

def logicCSLabBook(message):
    if(message.text==CSMand_listEdt[5]):
        return True
    else:
        return False
    
@bot.message_handler(func=logicCSLabQuest)
def send_logicCSLabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Vwbo1JTjkqZ4KOsSNPHej6yefTYmqiKI?usp=sharing")
@bot.message_handler(func=logicCSLabBook)
def send_logicCSLabBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/13gRDon6IzliuMURuQIei4DCND-486f0n?usp=sharing")

@bot.message_handler(func=dbCS)
def send_dbCS(message):
    dbCSMarkup=ReplyKeyboardMarkup()
    dbCSbtn=[]
    for i in range(6,9):
        dbCSbtn=KeyboardButton(CSMand_listEdt[i])
        dbCSMarkup.add(dbCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=dbCSMarkup)
def dbCSQuest(message):
    if(message.text==CSMand_listEdt[6]):
        return True
    else:
        return False
def dbCSSummary(message):
    if(message.text==CSMand_listEdt[7]):
        return True
    else:
        return False
def dbCSBook(message):
    if(message.text==CSMand_listEdt[8]):
        return True
    else:
        return False
@bot.message_handler(func=dbCSQuest)
def send_dbCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1pkiJEQKNeHHNS6w9VThTaP32kYqZ9WDP?usp=sharing")
@bot.message_handler(func=dbCSSummary)
def send_dbCSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1IVIMVz2sTO7cMZdQAQWEFjgkQYbSxXFy?usp=sharing")
@bot.message_handler(func=dbCSBook)
def send_dbCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1XTIkTEqq7P_Au7lycCnjyz_kguXUgTUD?usp=drive_link")

# Db CS lab
@bot.message_handler(func=dbCSLab)
def send_dbCSLab(message):
    dbCSLabMarkup=ReplyKeyboardMarkup()
    dbCSLabbtn=[]
    CSMand_listEdt[11]="سلايدات مختبر تصميم وادارة قواعد البيانات (1)"
    for i in range(9,12):
        dbCSLabbtn=KeyboardButton(CSMand_listEdt[i])
        dbCSLabMarkup.add(dbCSLabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=dbCSLabMarkup)
def dbCSLabQuest(message):
    if(message.text==CSMand_listEdt[9]):
        return True
    else:
        return False
def dbCSLabSummary(message):
    if(message.text==CSMand_listEdt[10]):
        return True
    else:
        return False
def dbCSLabSlides(message):
    if(message.text==CSMand_listEdt[11]):
        return True
    else:
        return False
@bot.message_handler(func=dbCSLabQuest)
def send_dbCSLabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Ow52V8UlzjYOG2_yz1cUfyCo73nikMq6?usp=drive_link")
@bot.message_handler(func=dbCSLabSummary)
def send_dbCSLabSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1TkmdG8vJpXV4EVhohKeqEOYx52W71XlC?usp=drive_link")
@bot.message_handler(func=dbCSLabSlides)
def send_dbCSLabSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1cyH4eVjAjixgE_ystZIIGFvMCnwp5C9Q?usp=drive_link")

#Algorithm CS
@bot.message_handler(func=algoCS)
def send_algoCS(message):
    algoCSMarkup=ReplyKeyboardMarkup()
    algoCSbtn=[]
    for i in range(12,15):
       algoCSbtn=KeyboardButton(CSMand_listEdt[i])
       algoCSMarkup.add(algoCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=algoCSMarkup)
def algoCSQuest(message):
    if(message.text==CSMand_listEdt[12]):
        return True
    else:
        return False
def algoCSSummary(message):
    if(message.text==CSMand_listEdt[13]):
        return True
    else:
        return False
def algoCSBook(message):
    if(message.text==CSMand_listEdt[14]):
        return True
    else:
        return False
@bot.message_handler(func=algoCSQuest)
def send_algoCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1qJucYNbNyvQuYPpCyX_NEtDUbg4x9UT9?usp=sharing")
@bot.message_handler(func=algoCSSummary)
def send_algoCSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/13Ww5QjlAQBw2MPRnT5AqNauyGVMjKXXg?usp=sharing") 
@bot.message_handler(func=algoCSBook)
def send_algoCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1rF9OAak8fCVqcfEL7W6F7O_r_7gHHdMT?usp=sharing")

#DS CS 

CSMand_listEdt[16]="سلايدات هياكل البيانات"
@bot.message_handler(func=DSCS)
def send_DSCS(message):
    DSCSMarkup=ReplyKeyboardMarkup()
    DSCSbtn=[]
    for i in range(15,18):
       DSCSbtn=KeyboardButton(CSMand_listEdt[i])
       DSCSMarkup.add(DSCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=DSCSMarkup)
def DSCSQuest(message):
    if(message.text==CSMand_listEdt[15]):
        return True
    else:
        return False
def DSCSslides(message):
    if(message.text==CSMand_listEdt[16]):
        return True
    else:
        return False
def DSCSBook(message):
    if(message.text==CSMand_listEdt[17]):
        return True
    else:
        return False
@bot.message_handler(func=DSCSslides)
def send_DSCSslides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1M8vOv68NqPxo9tWunn82xfSc3JwMG4Gz?usp=drive_link")
@bot.message_handler(func=DSCSQuest)
def send_DSCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1zpEaFyYo5l6d8E4NKmnP1ixJixJxLAcb?usp=drive_link") 
@bot.message_handler(func=DSCSBook)
def send_DSCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1KPFM3tD7AcZ9H-Gaq8C51DH_AWQCxqqM?usp=drive_link")
#DS CS LAB 

CSMand_listEdt[20]="سلايدات مختبر هياكل البيانات"
@bot.message_handler(func=DSCSlab)
def send_DSCSlab(message):
    DSCSlabMarkup=ReplyKeyboardMarkup()
    DSCSlabbtn=[]
    for i in range(18,21):
        if(i==19):
            continue
        DSCSlabbtn=KeyboardButton(CSMand_listEdt[i])
        DSCSlabMarkup.add(DSCSlabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=DSCSlabMarkup)
def DSCSlabQuest(message):
    if(message.text==CSMand_listEdt[18]):
        return True
    else:
        return False

def DSCSlabSlides(message):
    if(message.text==CSMand_listEdt[20]):
        return True
    else:
        return False
@bot.message_handler(func=DSCSlabQuest)
def send_DSCSlabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1X7RQbrClMj0TkczfaoYbhhKJ_0k47YfS?usp=drive_link")

@bot.message_handler(func=DSCSlabSlides)
def send_DSCSlabSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/14BdLdtkwva4EvIYExEXh4NfO7tJo7yPT?usp=drive_link")

#Android CS 

@bot.message_handler(func=AndCS)
def send_AndCS(message):
    AndCSMarkup=ReplyKeyboardMarkup()
    AndCSbtn=[]
    for i in range(21,24):
       AndCSbtn=KeyboardButton(CSMand_listEdt[i])
       AndCSMarkup.add(AndCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AndCSMarkup)
def AndCSQuest(message):
    if(message.text==CSMand_listEdt[21]):
        return True
    else:
        return False
def AndCSSummary(message):
    if(message.text==CSMand_listEdt[22]):
        return True
    else:
        return False
def AndCSBook(message):
    if(message.text==CSMand_listEdt[23]):
        return True
    else:
        return False
@bot.message_handler(func=AndCSQuest)
def send_AndCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1l6GQNARNSajxxFusqNfwILkatwTnk4Zn?usp=sharing")
@bot.message_handler(func=AndCSSummary)
def send_AndCSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1awyWBX_qXVXFLdjE-zZU2ArDk_HqGaX-?usp=sharing") 
@bot.message_handler(func=AndCSBook)
def send_AndCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1VnWfmrLQHSSQddIEs77j4ExWBq9ognNz?usp=sharing")

#Android CS LAB 

CSMand_listEdt[26]="سلايدات مختبر البرمجة المرئية للاجهزة الذكية"
@bot.message_handler(func=AndCSlab)
def send_AndCSlab(message):
    AndCSlabMarkup=ReplyKeyboardMarkup()
    AndCSlabbtn=[]
    for i in range(24,27):
        if(i==25):
            continue
        AndCSlabbtn=KeyboardButton(CSMand_listEdt[i])
        AndCSlabMarkup.add(AndCSlabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AndCSlabMarkup)
def AndCSlabQuest(message):
    if(message.text==CSMand_listEdt[24]):
        return True
    else:
        return False

def AndCSlabSlides(message):
    if(message.text==CSMand_listEdt[26]):
        return True
    else:
        return False
@bot.message_handler(func=AndCSlabQuest)
def send_AndCSlabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1deOBaSYwqcg6UQLzArzSV8dUkCA1Lgxb?usp=sharing")
@bot.message_handler(func=AndCSlabSlides)
def send_AndCSlabSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1IuZKQ4Y7kfE52kLa-YB8q0ZJ2mpktdXI?usp=sharing")

#AI CS 

@bot.message_handler(func=AICS)
def send_AICS(message):
    AICSMarkup=ReplyKeyboardMarkup()
    AICSbtn=[]
    for i in range(27,30):
       AICSbtn=KeyboardButton(CSMand_listEdt[i])
       AICSMarkup.add(AICSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AICSMarkup)
def AICSQuest(message):
    if(message.text==CSMand_listEdt[27]):
        return True
    else:
        return False
def AICSSummary(message):
    if(message.text==CSMand_listEdt[28]):
        return True
    else:
        return False
def AICSBook(message):
    if(message.text==CSMand_listEdt[29]):
        return True
    else:
        return False
@bot.message_handler(func=AICSQuest)
def send_AICSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1CMnRNSUcj-HOQZFHsCXakCimudsy3qV9?usp=drive_link")
@bot.message_handler(func=AICSSummary)
def send_AICSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1MfhaG3Xb2EGY8Ltw-9_chmZqp3prQEtT?usp=drive_link") 
@bot.message_handler(func=AICSBook)
def send_AICSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/11uZdXHqWu9mfR7Ad-PRLMyO71pld39Lj?usp=drive_link")

#Theory of Computation and Automata
@bot.message_handler(func=AutoCS)
def send_AutoCS(message):
    AutoCSMarkup=ReplyKeyboardMarkup()
    AutoCSbtn=[]
    for i in range(30,33):
       AutoCSbtn=KeyboardButton(CSMand_listEdt[i])
       AutoCSMarkup.add(AutoCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AutoCSMarkup)
def AutoCSQuest(message):
    if(message.text==CSMand_listEdt[30]):
        return True
    else:
        return False
def AutoCSSummary(message):
    if(message.text==CSMand_listEdt[31]):
        return True
    else:
        return False
def AutoCSBook(message):
    if(message.text==CSMand_listEdt[32]):
        return True
    else:
        return False
@bot.message_handler(func=AutoCSQuest)
def send_AutoCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1AXxwEQyuQ4tM3ceW9bOX5OgZ1lZFaE3Y?usp=sharing")
@bot.message_handler(func=AutoCSSummary)
def send_AutoCSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1ael8SOKW1oyYWE1WPOKb_E0tRpwB_Ff9?usp=sharing") 
@bot.message_handler(func=AutoCSBook)
def send_AutoCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1-lBmjzHzrVUadZawO8xS4_e1LBLyNgyM?usp=sharing")

#computer and network security

CSMand_listEdt[34]="سلايدات امن الحاسوب والشبكات"
@bot.message_handler(func=SecCS)
def send_SecCS(message):
    SecCSMarkup=ReplyKeyboardMarkup()
    SecCSbtn=[]
    for i in range(33,36):
        if(i==33):
            continue
        SecCSbtn=KeyboardButton(CSMand_listEdt[i])
        SecCSMarkup.add(SecCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=SecCSMarkup)

def SecCSSlides(message):
    if(message.text==CSMand_listEdt[34]):
        return True
    else:
        return False
def SecCSBook(message):
    if(message.text==CSMand_listEdt[35]):
        return True
    else:
        return False

@bot.message_handler(func=SecCSSlides)
def send_CSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1wEzSfyiw4LcGpPqTw3gDp8DSYObmAaSI?usp=sharing") 
@bot.message_handler(func=SecCSBook)
def send_SecCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1yuQ9Fy57VWcEgLfQVNWA_5MiYM2vrqZ5?usp=sharing")


#WEB CS 
@bot.message_handler(func=webCS)
def send_webCS(message):
    webCSMarkup=ReplyKeyboardMarkup()
    webCSbtn=[]
    for i in range(36,39):
       webCSbtn=KeyboardButton(CSMand_listEdt[i])
       webCSMarkup.add(webCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=webCSMarkup)
def webCSQuest(message):
    if(message.text==CSMand_listEdt[36]):
        return True
    else:
        return False
def webCSSummary(message):
    if(message.text==CSMand_listEdt[37]):
        return True
    else:
        return False
def webCSBook(message):
    if(message.text==CSMand_listEdt[38]):
        return True
    else:
        return False
@bot.message_handler(func=webCSQuest)
def send_webCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Hqin2jRGQ2xbqofcgnNI5c8g2Aop9QaA?usp=sharing")
@bot.message_handler(func=webCSSummary)
def send_webCSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1MHRAtNZvC6BtkO4UrOdZpoOGd9BzsB_N?usp=sharing") 
@bot.message_handler(func=webCSBook)
def send_webCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1A2LLw3Ur2EIBpT07FmwiTwTwz7PNUkAm?usp=sharing")

#WEB CS LAB 
CSMand_listEdt[41]="سلايدات مختبر برمجة تطبيقات الانترنت"
@bot.message_handler(func=webCSlab)
def send_webCSlab(message):
    webCSlabMarkup=ReplyKeyboardMarkup()
    webCSlabbtn=[]
    for i in range(39,42):
        if(i==40):
            continue
        webCSlabbtn=KeyboardButton(CSMand_listEdt[i])
        webCSlabMarkup.add(webCSlabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=webCSlabMarkup)
def webCSlabQuest(message):
    if(message.text==CSMand_listEdt[39]):
        return True
    else:
        return False
def webCSlabSlides(message):
    if(message.text==CSMand_listEdt[41]):
        return True
    else:
        return False
@bot.message_handler(func=webCSlabQuest)
def send_webCSlabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1PO7XgEp0_84MtNnI3iHnpUeD79fOd85w?usp=sharing")

@bot.message_handler(func=webCSlabSlides)
def send_webCSlabSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/155O2peuPuNJCIyOO7rn0GyPhypO8qGM_?usp=sharing")

#Principles of computer network
@bot.message_handler(func=netCS)
def send_netCS(message):
    netCSMarkup=ReplyKeyboardMarkup()
    netCSbtn=[]
    for i in range(51,54):
       netCSbtn=KeyboardButton(CSMand_listEdt[i])
       netCSMarkup.add(netCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=netCSMarkup)
def netCSQuest(message):
    if(message.text==CSMand_listEdt[51]):
        return True
    else:
        return False
def netCSSummary(message):
    if(message.text==CSMand_listEdt[52]):
        return True
    else:
        return False
def netCSBook(message):
    if(message.text==CSMand_listEdt[53]):
        return True
    else:
        return False
@bot.message_handler(func=netCSQuest)
def send_netCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/11P6FXmlp2fTNYwOVW-0NOYi6a1rGTxkd?usp=sharing")
@bot.message_handler(func=netCSSummary)
def send_netCSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1vVPSr8eDbk71A7K9OOtKw5A2RyWWyjRz?usp=sharing") 
@bot.message_handler(func=netCSBook)
def send_netCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1E57B2Sjl__hael5SCs0LRIN93L2XB3_K?usp=sharing")

#SA
@bot.message_handler(func=SACS)
def send_SACS(message):
    SACSMarkup=ReplyKeyboardMarkup()
    SACSbtn=[]
    for i in range(42,45):
       SACSbtn=KeyboardButton(CSMand_listEdt[i])
       SACSMarkup.add(SACSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=SACSMarkup)
def SACSQuest(message):
    if(message.text==CSMand_listEdt[42]):
        return True
    else:
        return False
def SACSSummary(message):
    if(message.text==CSMand_listEdt[43]):
        return True
    else:
        return False
def SACSBook(message):
    if(message.text==CSMand_listEdt[44]):
        return True
    else:
        return False
@bot.message_handler(func=SACSQuest)
def send_SACSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1TPeFh-t0VhnG5GFL9jC2Kf8WqQq6T1BM?usp=sharing")
@bot.message_handler(func=SACSSummary)
def send_SACSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1eAaVhblD6c8EuPGd5JnqkE6vVbLZ57bw?usp=sharing") 
@bot.message_handler(func=SACSBook)
def send_SACSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1XCvjGu2NSevMQiyW64sZsvSN5hRwghbk?usp=sharing")

#EMB CS
@bot.message_handler(func=EmbCS)
def send_EmbCS(message):
    EmbCSMarkup=ReplyKeyboardMarkup()
    EmbCSbtn=[]
    for i in range(45,48):
        if(i==46):
            continue
        EmbCSbtn=KeyboardButton(CSMand_listEdt[i])
        EmbCSMarkup.add(EmbCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=EmbCSMarkup)
def EmbCSQuest(message):
    if(message.text==CSMand_listEdt[45]):
        return True
    else:
        return False
def EmbCSBook(message):
    if(message.text==CSMand_listEdt[47]):
        return True
    else:
        return False
@bot.message_handler(func=EmbCSQuest)
def send_EmbCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1q5pz506_PBf82bf0J5BZr_YR3mKJkK8T?usp=sharing")

@bot.message_handler(func=EmbCSBook)
def send_EmbCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1s15eNkdobjnSuG4MQ4x2ERx-LwdGL5sA?usp=sharing")

#Wirless CS
@bot.message_handler(func=WirlessCS)
def send_WirlessCS(message):
    WirlessCSMarkup=ReplyKeyboardMarkup()
    WirlessCSbtn=[]
    for i in range(48,51):
       WirlessCSbtn=KeyboardButton(CSMand_listEdt[i])
       WirlessCSMarkup.add(WirlessCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=WirlessCSMarkup)
def WirlessCSQuest(message):
    if(message.text==CSMand_listEdt[48]):
        return True
    else:
        return False
def WirlessCSSummary(message):
    if(message.text==CSMand_listEdt[49]):
        return True
    else:
        return False
def WirlessCSBook(message):
    if(message.text==CSMand_listEdt[50]):
        return True
    else:
        return False
@bot.message_handler(func=WirlessCSQuest)
def send_WirlessCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1GQ7YqSXf4hZ0SqzQyXp1SI-akmWgjY92?usp=drive_link")
@bot.message_handler(func=WirlessCSSummary)
def send_WirlessCSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1sILGfu8WU_mBTW7DUSe0V8Loc59auBnx?usp=drive_link") 
@bot.message_handler(func=WirlessCSBook)
def send_WirlessCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Ks_YA8tvM4935ydHwA9e_lH0XtHblsf-?usp=drive_link")

#Compiler CS

@bot.message_handler(func=CompilerCS)
def send_CS(message):
    CompilerCSMarkup=ReplyKeyboardMarkup()
    CompilerCSbtn=[]
    for i in range(54,57):
       CompilerCSbtn=KeyboardButton(CSMand_listEdt[i])
       CompilerCSMarkup.add(CompilerCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=CompilerCSMarkup)
def CompilerCSQuest(message):
    if(message.text==CSMand_listEdt[54]):
        return True
    else:
        return False
def CompilerCSSummary(message):
    if(message.text==CSMand_listEdt[55]):
        return True
    else:
        return False
def CompilerCSBook(message):
    if(message.text==CSMand_listEdt[56]):
        return True
    else:
        return False
@bot.message_handler(func=CompilerCSQuest)
def send_CompilerCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/15jknS2EbY7grK_R6IkK18zfeNqJgwzAO?usp=sharing")
@bot.message_handler(func=CompilerCSSummary)
def send_CompilerCSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1U_QcnxjON7IaTNKorIvMSK8ZURZSc1n5?usp=sharing") 
@bot.message_handler(func=CompilerCSBook)
def send_CompilerCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1f0WY6Shxc9bIvER0itCYZ1_EdNcw-Phg?usp=sharing")

#OS CS 
@bot.message_handler(func=OSCS)
def send_OSCS(message):
    OSCSMarkup=ReplyKeyboardMarkup()
    OSCSbtn=[]
    for i in range(57,60):
       OSCSbtn=KeyboardButton(CSMand_listEdt[i])
       OSCSMarkup.add(OSCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=OSCSMarkup)
def OSCSQuest(message):
    if(message.text==CSMand_listEdt[57]):
        return True
    else:
        return False
def OSCSSummary(message):
    if(message.text==CSMand_listEdt[58]):
        return True
    else:
        return False
def OSCSBook(message):
    if(message.text==CSMand_listEdt[59]):
        return True
    else:
        return False
@bot.message_handler(func=OSCSQuest)
def send_OSCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Jowd4a4CjGLGiGplfrTlHaC-zPaRTPJD?usp=sharing")
@bot.message_handler(func=OSCSSummary)
def send_OSCSSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/14kOC1LaCOpN6cYHzFLecmgZCzcxvZC_k?usp=sharing") 
@bot.message_handler(func=OSCSBook)
def send_OSCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1jgiGnfHy9ltax2aMS7yLMx9XblmFowOf?usp=sharing")

#ARCH CS 
CSMand_listEdt[61]="سلايدات معمارية الحاسوب"
@bot.message_handler(func=ArchCS)
def send_ArchCS(message):
    ArchCSMarkup=ReplyKeyboardMarkup()
    ArchCSbtn=[]
    for i in range(60,63):
       ArchCSbtn=KeyboardButton(CSMand_listEdt[i])
       ArchCSMarkup.add(ArchCSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=ArchCSMarkup)
def ArchCSQuest(message):
    if(message.text==CSMand_listEdt[60]):
        return True
    else:
        return False
def ArchCSSlides(message):
    if(message.text==CSMand_listEdt[61]):
        return True
    else:
        return False
def ArchCSBook(message):
    if(message.text==CSMand_listEdt[62]):
        return True
    else:
        return False
@bot.message_handler(func=ArchCSQuest)
def send_ArchCSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1f04FIlmb1jqNfkEnEUFKZDuPB2h-g3qH?usp=sharing")
@bot.message_handler(func=ArchCSSlides)
def send_ArchCSSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1ov6HBiJYhMuEYEEb9xofh-jVGXDCj7dp?usp=sharing") 
@bot.message_handler(func=ArchCSBook)
def send_ArchCSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1WJsyltuSZlU3USpB_56RpeoQSjn_6lxi?usp=sharing")

#Modern Software Engineering

CSMand_listEdt[64]="سلايدات هندسة البرمجيات الحديثة"
@bot.message_handler(func=MSECS)
def send_MSECS(message):
    MSECSMarkup=ReplyKeyboardMarkup()
    MSECSbtn=[]
    for i in range(63,66):
       MSECSbtn=KeyboardButton(CSMand_listEdt[i])
       MSECSMarkup.add(MSECSbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=MSECSMarkup)
def MSECSQuest(message):
    if(message.text==CSMand_listEdt[63]):
        return True
    else:
        return False
def MSECSSlides(message):
    if(message.text==CSMand_listEdt[64]):
        return True
    else:
        return False
def MSECSBook(message):
    if(message.text==CSMand_listEdt[65]):
        return True
    else:
        return False
@bot.message_handler(func=MSECSQuest)
def send_MSECSQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1jeEZcDFNjbkjpJO5IaFTi3dNsz_bmnwN?usp=sharing")
@bot.message_handler(func=MSECSSlides)
def send_MSECSSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1-K-zH9txjaiFFnY--16VLQRbNakJzvQS?usp=sharing") 
@bot.message_handler(func=MSECSBook)
def send_MSECSBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/19INCq_N1twnSmRgcvOY7xBxr6qcNhp93?usp=sharing")



#################################
#########CIS Mandatory###########
#################################
@bot.message_handler(func=CIS)
def send_CISmand(message):
    CISMandMarkup=ReplyKeyboardMarkup()
    CISMandbtn=[]
    for i in range(len(CISMand_list)):
        if(i%2==0 and i<len(CISMand_list)):
            CISMandbtn=KeyboardButton(CISMand_list[i])
            CISMandbtn2=KeyboardButton(CISMand_list[i+1])
            CISMandMarkup.add(CISMandbtn,CISMandbtn2)
        else:
            CISMandbtn=KeyboardButton(CISMand_list[i])
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=CISMandMarkup)
def logicCIS(message):
    if(message.text==CISMand_list[0]):
        return True
    else:
        return False
def logicCISLab(message):
    if(message.text==CISMand_list[1]):
        return True
    else:
        return False
def dbCIS(message):
    if(message.text==CISMand_list[3]):
        return True
    else:
        return False
def dbCISLab(message):
    if(message.text==CISMand_list[4]):
        return True
    else:
        return False
def DSCIS(message):
    if(message.text==CISMand_list[5]):
        return True
    else:
        return False
def DSCISlab(message):
    if(message.text==CISMand_list[6]):
        return True
    else:
        return False    
def algoCIS(message):
    if(message.text==CISMand_list[7]):
        return True 
    else:
        return False   
def AdvanceCIS(message):
    if(message.text==CISMand_list[8]):
        return True
    else:
        return False
def AdvanceLabCIS(message):
    if(message.text==CISMand_list[9]):
        return True
    else:
        return False
def AndCIS(message):
    if(message.text==CISMand_list[10]):
        return True
    else:
        return False
def AndCISlab(message):
    if(message.text==CISMand_list[11]):
        return True
    else:
        return False
def MiningCIS(message):
    if(message.text==CISMand_list[12]):
        return True
    else:
        return False   
def MiningLabCIS(message):
    if(message.text==CISMand_list[13]):
        return True
    else:
        return False
def webCIS(message):
    if(message.text==CISMand_list[15]):
        return True
    else:
        return False
def webCISlab(message):
    if(message.text==CISMand_list[16]):
        return True
    else:
        return False
def WarehouseCIS(message):
    if(message.text==CISMand_list[17]):
        return True
    else:
        return False
def IRCIS(message):
    if(message.text==CISMand_list[18]):
        return True
    else:
        return False
def OSCIS(message):
    if(message.text==CISMand_list[19]):
        return True
    else:
        return False
def CNMCIS(message):
    if(message.text==CISMand_list[20]):
        return True
    else:
        return False
def ISCIS(message):
    if(message.text==CISMand_list[21]):
        return True
    else:
        return False
def WirelessCIS(message):
    if(message.text==CISMand_list[22]):
        return True
    else:
        return False
#logic CIS
@bot.message_handler(func=logicCIS)
def send_logicCIS(message):
    logicCISMarkup=ReplyKeyboardMarkup()
    logicCISbtn=[]
    for i in range(3):
        logicCISbtn=KeyboardButton(CISMand_listEdt[i])
        logicCISMarkup.add(logicCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=logicCISMarkup)
def logicCISQuest(message):
    if(message.text==CISMand_listEdt[0]):
        return True
    else:
        return False
def logicCISSummary(message):
    if(message.text==CISMand_listEdt[1]):
        return True
    else:
        return False
def logicCISBook(message):
    if(message.text==CISMand_listEdt[2]):
        return True
    else:
        return False
@bot.message_handler(func=logicCISQuest)
def send_logicCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1SqSqbBqUho84Tay84GBLYNwTuxoVsgcT?usp=sharing")
@bot.message_handler(func=logicCISSummary)
def send_logicCISSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1cp0cEaR5J-_IiD5tYagHb-QuS5kNsVtv?usp=sharing")
@bot.message_handler(func=logicCISBook)
def send_logicCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1tQ2QFiNMznTkqlAosViQpcmIMMp8W9-h?usp=sharing")

#logic CIS Lab
@bot.message_handler(func=logicCISLab)
def send_logicCISLab(message):
    logicCISLabMarkup=ReplyKeyboardMarkup()
    logicCISLabbtn=[]
    for i in range(3,6):
        if(i==4):
            continue
        logicCISLabbtn=KeyboardButton(CISMand_listEdt[i])
        logicCISLabMarkup.add(logicCISLabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=logicCISLabMarkup)
def logicCISLabQuest(message):
    if(message.text==CISMand_listEdt[3]):
        return True
    else:
        return False
def logicCISLabBook(message):
    if(message.text==CISMand_listEdt[5]):
        return True
    else:
        return False
@bot.message_handler(func=logicCISLabQuest)
def send_logicCISLabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1SoR32LmBpMCM9RXy1F7vsmY0EP0T6OP5?usp=sharing")

@bot.message_handler(func=logicCISLabBook)
def send_logicCISLabBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1W2QdPAvKAg1sC7AHU-auRZBWF125_LT3?usp=sharing")

#DB CIS 
@bot.message_handler(func=dbCIS)
def send_dbCIS(message):
    dbCISMarkup=ReplyKeyboardMarkup()
    dbCISbtn=[]
    for i in range(9,12):
        dbCISbtn=KeyboardButton(CISMand_listEdt[i])
        dbCISMarkup.add(dbCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=dbCISMarkup)
def dbCISQuest(message):
    if(message.text==CISMand_listEdt[9]):
        return True
    else:
        return False
def dbCISSummary(message):
    if(message.text==CISMand_listEdt[10]):
        return True
    else:
        return False
def dbCISBook(message):
    if(message.text==CISMand_listEdt[11]):
        return True
    else:
        return False
@bot.message_handler(func=dbCISQuest)
def send_dbCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1pkiJEQKNeHHNS6w9VThTaP32kYqZ9WDP?usp=drive_link")
@bot.message_handler(func=dbCISSummary)
def send_dbCISSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1IVIMVz2sTO7cMZdQAQWEFjgkQYbSxXFy?usp=drive_link")
@bot.message_handler(func=dbCISBook)
def send_dbCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1XTIkTEqq7P_Au7lycCnjyz_kguXUgTUD?usp=drive_link")

# Db CIS lab
@bot.message_handler(func=dbCISLab)
def send_dbCISLab(message):
    dbCISLabMarkup=ReplyKeyboardMarkup()
    dbCISLabbtn=[]
    CISMand_listEdt[14]="سلايدات مختبر نماذج وقواعد البيانات"
    for i in range(12,15):
        dbCISLabbtn=KeyboardButton(CISMand_listEdt[i])
        dbCISLabMarkup.add(dbCISLabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=dbCISLabMarkup)
def dbCISLabQuest(message):
    if(message.text==CISMand_listEdt[12]):
        return True
    else:
        return False
def dbCISLabSummary(message):
    if(message.text==CISMand_listEdt[13]):
        return True
    else:
        return False
def dbCISLabSlides(message):
    if(message.text==CISMand_listEdt[14]):
        return True
    else:
        return False
@bot.message_handler(func=dbCISLabQuest)
def send_dbCISLabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Ow52V8UlzjYOG2_yz1cUfyCo73nikMq6?usp=drive_link")
@bot.message_handler(func=dbCISLabSummary)
def send_dbCISLabSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1TkmdG8vJpXV4EVhohKeqEOYx52W71XlC?usp=drive_link")
@bot.message_handler(func=dbCISLabSlides)
def send_dbCISLabSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1cyH4eVjAjixgE_ystZIIGFvMCnwp5C9Q?usp=drive_link")

#DS CIS 


CISMand_listEdt[16]="سلايدات هياكل البيانات والملفات (1)"
@bot.message_handler(func=DSCIS)
def send_DSCIS(message):
    DSCISMarkup=ReplyKeyboardMarkup()
    DSCISbtn=[]
    for i in range(15,18):
       DSCISbtn=KeyboardButton(CISMand_listEdt[i])
       DSCISMarkup.add(DSCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=DSCISMarkup)

def DSCISQuest(message):
    if(message.text==CISMand_listEdt[15]):
        return True
    else:
        return False    

def DSCISslides(message):
    if(message.text==CISMand_listEdt[16]):
        return True
    else:
        return False

def DSCISBook(message):
    if(message.text==CISMand_listEdt[17]):
        return True
    else:
        return False
@bot.message_handler(func=DSCISslides)
def send_DSCISslides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1M8vOv68NqPxo9tWunn82xfSc3JwMG4Gz?usp=drive_link")
@bot.message_handler(func=DSCISQuest)
def send_DSCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1zpEaFyYo5l6d8E4NKmnP1ixJixJxLAcb?usp=drive_link") 
@bot.message_handler(func=DSCISBook)
def send_DSCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1KPFM3tD7AcZ9H-Gaq8C51DH_AWQCxqqM?usp=drive_link")

#DS CIS LAB

CISMand_listEdt[20]="سلايدات مختبر هياكل البيانات والملفات (1)"
@bot.message_handler(func=DSCISlab)
def send_DSCISlab(message):
    DSCISlabMarkup=ReplyKeyboardMarkup()
    DSCISlabbtn=[]
    for i in range(18,21):
        if(i==19):
            continue
        DSCISlabbtn=KeyboardButton(CISMand_listEdt[i])
        DSCISlabMarkup.add(DSCISlabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=DSCISlabMarkup)
def DSCISlabQuest(message):
    if(message.text==CISMand_listEdt[18]):
        return True
    else:
        return False

def DSCISlabSlides(message):
    if(message.text==CISMand_listEdt[20]):
        return True
    else:
        return False
@bot.message_handler(func=DSCISlabQuest)
def send_DSCISlabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1X7RQbrClMj0TkczfaoYbhhKJ_0k47YfS?usp=drive_link")

@bot.message_handler(func=DSCISlabSlides)
def send_DSCISlabSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/14BdLdtkwva4EvIYExEXh4NfO7tJo7yPT?usp=drive_link")


#Algorithm CIS 

@bot.message_handler(func=algoCIS)
def send_algoCIS(message):
    algoCISMarkup=ReplyKeyboardMarkup()
    algoCISbtn=[]
    for i in range(21,24):
       algoCISbtn=KeyboardButton(CISMand_listEdt[i])
       algoCISMarkup.add(algoCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=algoCISMarkup)
def algoCISQuest(message):
    if(message.text==CISMand_listEdt[21]):
        return True
    else:
        return False
def algoCISSummary(message):
    if(message.text==CISMand_listEdt[22]):
        return True
    else:
        return False
def algoCISBook(message):
    if(message.text==CISMand_listEdt[23]):
        return True
    else:
        return False
@bot.message_handler(func=algoCISQuest)
def send_algoCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1qJucYNbNyvQuYPpCyX_NEtDUbg4x9UT9?usp=sharing")
@bot.message_handler(func=algoCISSummary)
def send_algoCISSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/13Ww5QjlAQBw2MPRnT5AqNauyGVMjKXXg?usp=sharing") 
@bot.message_handler(func=algoCISBook)
def send_algoCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1rF9OAak8fCVqcfEL7W6F7O_r_7gHHdMT?usp=sharing")

#Advance CIS 

CISMand_listEdt[25]="سلايدات وواجبات ادارة نظم قواعد البيانات"
@bot.message_handler(func=AdvanceCIS)
def send_Advance(message):
    AdvanceMarkup=ReplyKeyboardMarkup()
    Advancebtn=[]
    for i in range(24,27):
       Advancebtn=KeyboardButton(CISMand_listEdt[i])
       AdvanceMarkup.add(Advancebtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AdvanceMarkup)
def AdvanceQuest(message):
    if(message.text==CISMand_listEdt[24]):
        return True
    else:
        return False
def AdvanceSummary(message):
    if(message.text==CISMand_listEdt[25]):
        return True
    else:
        return False
def AdvanceBook(message):
    if(message.text==CISMand_listEdt[26]):
        return True
    else:
        return False
@bot.message_handler(func=AdvanceQuest)
def send_CISoptQuest(message):
    bot.send_message(message.chat.id,text="الكويز الاول : https://drive.google.com/drive/folders/13ZEWnZwaThDn9sxPtWJKBDGj9jlUzp3y?usp=drive_link")
@bot.message_handler(func=AdvanceSummary)
def send_CISoptSummary(message):
    bot.send_message(message.chat.id,text="سلايدات : https://drive.google.com/drive/folders/1aje4Px83Q2s_OS3Jq-DTtX3IzOR6fiha?usp=drive_link") 
    bot.send_message(message.chat.id,text="واجبات : https://drive.google.com/drive/folders/1OUiEUVcvEhX8UtZxOAbS4uIOa6vVl3tm?usp=drive_link")
@bot.message_handler(func=AdvanceBook)
def send_CISoptBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1n47YV713TBaqF7OSCl17FNC0lFacTfNm?usp=drive_link")

#Advance Lab CIS 
@bot.message_handler(func=AdvanceLabCIS)
def send_AdvanceLabCIS(message):
    bot.send_message(message.chat.id,text="المعذرة لا نملك معلومات عن هذه المادة ان وجد لديك ماا قد يفيدنا ويفيد الطلاب يرجى مراسلة صاحب البوت ")


#Android CIS 

@bot.message_handler(func=AndCIS)
def send_AndCIS(message):
    AndCISMarkup=ReplyKeyboardMarkup()
    AndCISbtn=[]
    for i in range(30,33):
       AndCISbtn=KeyboardButton(CISMand_listEdt[i])
       AndCISMarkup.add(AndCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AndCISMarkup)
def AndCISQuest(message):
    if(message.text==CISMand_listEdt[30]):
        return True
    else:
        return False
def AndCISSummary(message):
    if(message.text==CISMand_listEdt[31]):
        return True
    else:
        return False
def AndCISBook(message):
    if(message.text==CISMand_listEdt[32]):
        return True
    else:
        return False
@bot.message_handler(func=AndCISQuest)
def send_AndCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1l6GQNARNSajxxFusqNfwILkatwTnk4Zn?usp=sharing")
@bot.message_handler(func=AndCISSummary)
def send_AndCISSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1awyWBX_qXVXFLdjE-zZU2ArDk_HqGaX-?usp=sharing") 
@bot.message_handler(func=AndCISBook)
def send_AndCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1VnWfmrLQHSSQddIEs77j4ExWBq9ognNz?usp=sharing")

#Android CIS LAB

CISMand_listEdt[35]="سلايدات مختبر البرمجة المرئية والواجهات الرسومية"
@bot.message_handler(func=AndCISlab)
def send_AndCISlab(message):
    AndCISlabMarkup=ReplyKeyboardMarkup()
    AndCISlabbtn=[]
    for i in range(33,36):
        if(i==34):
            continue
        AndCISlabbtn=KeyboardButton(CISMand_listEdt[i])
        AndCISlabMarkup.add(AndCISlabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AndCISlabMarkup)
def AndCISlabQuest(message):
    if(message.text==CISMand_listEdt[33]):
        return True
    else:
        return False

def AndCISlabSlides(message):
    if(message.text==CISMand_listEdt[35]):
        return True
    else:
        return False
@bot.message_handler(func=AndCISlabQuest)
def send_AndCISlabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1deOBaSYwqcg6UQLzArzSV8dUkCA1Lgxb?usp=sharing")
@bot.message_handler(func=AndCISlabSlides)
def send_AndCISlabSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1IuZKQ4Y7kfE52kLa-YB8q0ZJ2mpktdXI?usp=sharing")

#Data Mining CIS
CISMand_listEdt[37]="سلايدات التنقيب عن البيانات"
@bot.message_handler(func=MiningCIS)
def send_MiningCIS(message):
    MiningCISMarkup=ReplyKeyboardMarkup()
    MiningCISbtn=[]
    for i in range(36,39):
       MiningCISbtn=KeyboardButton(CISMand_listEdt[i])
       MiningCISMarkup.add(MiningCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=MiningCISMarkup)
def MiningCISQuest(message):
    if(message.text==CISMand_listEdt[36]):
        return True
    else:
        return False
def MiningCISSlides(message):
    if(message.text==CISMand_listEdt[37]):
        return True
    else:
        return False
def MiningCISBook(message):
    if(message.text==CISMand_listEdt[38]):
        return True
    else:
        return False
@bot.message_handler(func=MiningCISQuest)
def send_MiningCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1IoFkwPJ1aN-J1gKKLIzBPIx2VOKbiwbA?usp=share_link")
@bot.message_handler(func=MiningCISSlides)
def send_MiningCISSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1jswgwqIbBWWWGNiEOup8aQpWX19I9pxs?usp=share_link") 
@bot.message_handler(func=MiningCISBook)
def send_MiningCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1AM2GiPn0E5fec4aal_k9XP_tEJyIAlDq?usp=share_link")
    
#Mining Lab CIS
@bot.message_handler(func=MiningLabCIS)
def send_CISQuest(message):
    bot.send_message(message.chat.id,text="هذا الشيء عجز مخي عنه تعتمد حسب الدكتور اذا كان دكتورها ذيب يرجى التواصل معي")


#WEB CIS 
@bot.message_handler(func=webCIS)
def send_webCIS(message):
    webCISMarkup=ReplyKeyboardMarkup()
    webCISbtn=[]
    for i in range(45,48):
       webCISbtn=KeyboardButton(CISMand_listEdt[i])
       webCISMarkup.add(webCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=webCISMarkup)
def webCISQuest(message):
    if(message.text==CISMand_listEdt[45]):
        return True
    else:
        return False
def webCISSummary(message):
    if(message.text==CISMand_listEdt[46]):
        return True
    else:
        return False
def webCISBook(message):
    if(message.text==CISMand_listEdt[47]):
        return True
    else:
        return False
@bot.message_handler(func=webCISQuest)
def send_webCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Hqin2jRGQ2xbqofcgnNI5c8g2Aop9QaA?usp=sharing")
@bot.message_handler(func=webCISSummary)
def send_webCISSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1MHRAtNZvC6BtkO4UrOdZpoOGd9BzsB_N?usp=sharing") 
@bot.message_handler(func=webCISBook)
def send_webCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1A2LLw3Ur2EIBpT07FmwiTwTwz7PNUkAm?usp=sharing")

#WEB CIS LAB 
CISMand_listEdt[50]="سلايدات مختبر تطبيقات وخدمات الويب"
@bot.message_handler(func=webCISlab)
def send_webCIISlab(message):
    webCISlabMarkup=ReplyKeyboardMarkup()
    webCISlabbtn=[]
    for i in range(48,51):
        if(i==49):
            continue
        webCISlabbtn=KeyboardButton(CISMand_listEdt[i])
        webCISlabMarkup.add(webCISlabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=webCISlabMarkup)
def webCISlabQuest(message):
    if(message.text==CISMand_listEdt[48]):
        return True
    else:
        return False
def webCISlabSlides(message):
    if(message.text==CISMand_listEdt[50]):
        return True
    else:
        return False
@bot.message_handler(func=webCISlabQuest)
def send_webCISlabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1PO7XgEp0_84MtNnI3iHnpUeD79fOd85w?usp=sharing")

@bot.message_handler(func=webCISlabSlides)
def send_webCISlabSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/155O2peuPuNJCIyOO7rn0GyPhypO8qGM_?usp=sharing")
#Data Warehouse CIS
@bot.message_handler(func=WarehouseCIS)
def send_WarehouseCIS(message):
    bot.send_message(message.chat.id,text="تغيرت المادة علينا للاسف 💔💔\n")
    bot.send_message(message.chat.id,text="ان كنت تملك اي شيء عنها يرجى التواصل مع صاحب البوت ")
#IR CIS 
CISMand_listEdt[55]="سلايدات نظم استرجاع المعلومات"
@bot.message_handler(func=IRCIS)
def send_CIS(message):
    IRCISMarkup=ReplyKeyboardMarkup()
    IRCISbtn=[]
    for i in range(54,57):
       IRCISbtn=KeyboardButton(CISMand_listEdt[i])
       IRCISMarkup.add(IRCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=IRCISMarkup)
def IRCISQuest(message):
    if(message.text==CISMand_listEdt[54]):
        return True
    else:
        return False
def IRCISSlides(message):
    if(message.text==CISMand_listEdt[55]):
        return True
    else:
        return False
def IRCISBook(message):
    if(message.text==CISMand_listEdt[56]):
        return True
    else:
        return False
@bot.message_handler(func=IRCISQuest)
def send_IRCISQuest(message):
    bot.send_message(message.chat.id,text="اسئلة سنوات :\n"+"https://drive.google.com/drive/folders/1cO7_8ckMajpmMR_KFuJVeBjffGVouTd4?usp=share_link")
    bot.send_message(message.chat.id,text="اسئلة كويزات :\n"+ "https://drive.google.com/drive/folders/1QwVs7QW6CXDYHAE7-7ivzNyAj40MpGoM?usp=share_link")
@bot.message_handler(func=IRCISSlides)
def send_IRCISSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1eJn03VMrChNTq0JkTQn5SjlgfEQ-9tzQ?usp=share_link") 
@bot.message_handler(func=IRCISBook)
def send_IRCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1maD75r3Iz5n8Pk-B2TjSjb6de6d10NgK?usp=share_link")

#OS CIS 
@bot.message_handler(func=OSCIS)
def send_OSCIS(message):
    OSCISMarkup=ReplyKeyboardMarkup()
    OSCISbtn=[]
    for i in range(57,60):
       OSCISbtn=KeyboardButton(CISMand_listEdt[i])
       OSCISMarkup.add(OSCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=OSCISMarkup)
def OSCISQuest(message):
    if(message.text==CISMand_listEdt[57]):
        return True
    else:
        return False
def OSCISSummary(message):
    if(message.text==CISMand_listEdt[58]):
        return True
    else:
        return False
def OSCISBook(message):
    if(message.text==CISMand_listEdt[59]):
        return True
    else:
        return False
@bot.message_handler(func=OSCISQuest)
def send_OSCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Jowd4a4CjGLGiGplfrTlHaC-zPaRTPJD?usp=sharing")
@bot.message_handler(func=OSCISSummary)
def send_OSCISSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/14kOC1LaCOpN6cYHzFLecmgZCzcxvZC_k?usp=sharing") 
@bot.message_handler(func=OSCISBook)
def send_OSCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1jgiGnfHy9ltax2aMS7yLMx9XblmFowOf?usp=sharing")

#CNM CIS
CISMand_listEdt[61]="سلايدات ادارة شبكات الحاسوب"
@bot.message_handler(func=CNMCIS)
def send_CNMCIS(message):
    CNMCISMarkup=ReplyKeyboardMarkup()
    CNMCISbtn=[]
    for i in range(60,63):
       CNMCISbtn=KeyboardButton(CISMand_listEdt[i])
       CNMCISMarkup.add(CNMCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=CNMCISMarkup)
def CNMCISQuest(message):
    if(message.text==CISMand_listEdt[60]):
        return True
    else:
        return False
def CNMCISSlides(message):
    if(message.text==CISMand_listEdt[61]):
        return True
    else:
        return False
def CNMCISBook(message):
    if(message.text==CISMand_listEdt[62]):
        return True
    else:
        return False
@bot.message_handler(func=CNMCISQuest)
def send_CNMCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1r2CQuXBLdtYutY4Xb64iOP000yK203RV?usp=share_link")
@bot.message_handler(func=CNMCISSlides)
def send_CNMCISSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Ht9Yb3iXDfAL4maPWaov3FXMmnOfUUck?usp=share_link") 
@bot.message_handler(func=CNMCISBook)
def send_CNMCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1BQU6tISpj402B_HbG_5qMSvPP-Cvi1n1?usp=share_link")

#IS CIS
CISMand_listEdt[64]="سلايدات امن المعلومات"
@bot.message_handler(func=ISCIS)
def send_ISCIS(message):
    ISCISMarkup=ReplyKeyboardMarkup()
    ISCISbtn=[]
    for i in range(63,66):
        if(i==65):
            continue
        ISCISbtn=KeyboardButton(CISMand_listEdt[i])
        ISCISMarkup.add(ISCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=ISCISMarkup)
def ISCISQuest(message):
    if(message.text==CISMand_listEdt[63]):
        return True
    else:
        return False
def ISCISSlides(message):
    if(message.text==CISMand_listEdt[64]):
        return True
    else:
        return False
@bot.message_handler(func=ISCISQuest)
def send_ISCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1OH0dyh2QN3MngIA0D5efW7Vndew-pp-p?usp=share_link")
@bot.message_handler(func=ISCISSlides)
def send_ISCISSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1ZYeZ8StBusXvEdOJi3ZxO13jT0Xbgs9H?usp=share_link") 

#Wireless CIS
@bot.message_handler(func=WirelessCIS)
def send_WirelessCIS(message):
    WirelessCISMarkup=ReplyKeyboardMarkup()
    WirelessCISbtn=[]
    for i in range(66,69):
       WirelessCISbtn=KeyboardButton(CISMand_listEdt[i])
       WirelessCISMarkup.add(WirelessCISbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=WirelessCISMarkup)
def WirelessCISQuest(message):
    if(message.text==CISMand_listEdt[66]):
        return True
    else:
        return False
def WirelessCISSummary(message):
    if(message.text==CISMand_listEdt[67]):
        return True
    else:
        return False
def WirelessCISBook(message):
    if(message.text==CISMand_listEdt[68]):
        return True
    else:
        return False
@bot.message_handler(func=WirelessCISQuest)
def send_WirelessCISQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1GQ7YqSXf4hZ0SqzQyXp1SI-akmWgjY92?usp=share_link")
@bot.message_handler(func=WirelessCISSummary)
def send_WirelessCISSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1sILGfu8WU_mBTW7DUSe0V8Loc59auBnx?usp=share_link") 
@bot.message_handler(func=WirelessCISBook)
def send_WirelessCISBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Ks_YA8tvM4935ydHwA9e_lH0XtHblsf-?usp=share_link")



########################
######SE Mandatory######
########################
@bot.message_handler(func=SE)
def send_SEmand(message):
    SEMandMarkup=ReplyKeyboardMarkup()
    SEMandbtn=[]
    for i in range(len(SEMand_list)):
        if(i%2==0 and i<len(SEMand_list)-1):
            SEMandbtn=KeyboardButton(SEMand_list[i])
            SEMandbtn2=KeyboardButton(SEMand_list[i+1])
            SEMandMarkup.add(SEMandbtn,SEMandbtn2)
        else:
            SEMandbtn=KeyboardButton(SEMand_list[i])
    SEMandMarkup.add(KeyboardButton(SEMand_list[20]))
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=SEMandMarkup)

def dbSE(message):
    if(message.text==SEMand_list[0]):
        return True
    else:
        return False
def dbSELab(message):
    if(message.text==SEMand_list[1]):
        return True
    else:
        return False
def ToolSE(message):
    if(message.text==SEMand_list[2]):
        return True
    else:
        return False
def TestSE(message):
    if(message.text==SEMand_list[3]):
        return True
    else:
        return False
def AndSE(message):
    if(message.text==SEMand_list[4]):
        return True
    else:
        return False
def AndSElab(message):
    if(message.text==SEMand_list[5]):
        return True
    else:
        return False
def algoSE(message):
    if(message.text==SEMand_list[8]):
        return True
    else:
        return False
def planSE(message):
    if(message.text==SEMand_list[9]):
        return True
    else:
        return False
def designCompSE(message):
    if(message.text==SEMand_list[10]):
        return True
    else:
        return False
def usrinterSE(message):
    if(message.text==SEMand_list[11]):
        return True
    else:
        return False
def webSE(message):
    if(message.text==SEMand_list[12]):
        return True
    else:
        return False
def componentSE(message):
    if(message.text==SEMand_list[13]):
        return True
    else:
        return False
def principleSESE(message):
    if(message.text==SEMand_list[15]):
        return True
    else:
        return False
def DSSE(message):
    if(message.text==SEMand_list[16]):
        return True
    else:
        return False
def DSSElab(message):
    if(message.text==SEMand_list[17]):
        return True
    else:
        return False
def OSSE(message):
    if(message.text==SEMand_list[18]):
        return True
    else:
        return False
def objectSE(message):
    if(message.text==SEMand_list[19]):
        return True
    else:
        return False

def reqSE(message):
    if(message.text==SEMand_list[20]):
        return True
    else:
        return False

#DB SE
@bot.message_handler(func=dbSE)
def send_dbSE(message):
    dbSEMarkup=ReplyKeyboardMarkup()
    dbSEbtn=[]
    for i in range(3):
        dbSEbtn=KeyboardButton(SEMand_listEdt[i])
        dbSEMarkup.add(dbSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=dbSEMarkup)
def dbSEQuest(message):
    if(message.text==SEMand_listEdt[0]):
        return True
    else:
        return False
def dbSESummary(message):
    if(message.text==SEMand_listEdt[1]):
        return True
    else:
        return False
def dbSEBook(message):
    if(message.text==SEMand_listEdt[2]):
        return True
    else:
        return False
@bot.message_handler(func=dbSEQuest)
def send_dbSEQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1pkiJEQKNeHHNS6w9VThTaP32kYqZ9WDP?usp=drive_link")
@bot.message_handler(func=dbSESummary)
def send_dbSESummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1IVIMVz2sTO7cMZdQAQWEFjgkQYbSxXFy?usp=drive_link")
@bot.message_handler(func=dbSEBook)
def send_dbSEBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1XTIkTEqq7P_Au7lycCnjyz_kguXUgTUD?usp=drive_link")

# DB SE Lab
@bot.message_handler(func=dbSELab)
def send_dbSELab(message):
    dbSELabMarkup=ReplyKeyboardMarkup()
    dbSELabbtn=[]
    SEMand_listEdt[5]="سلايدات مختبر قواعد البيانات"
    for i in range(3,6):
        if(i==4):
            continue
        dbSELabbtn=KeyboardButton(SEMand_listEdt[i])
        dbSELabMarkup.add(dbSELabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=dbSELabMarkup)
def dbSELabQuest(message):
    if(message.text==SEMand_listEdt[3]):
        return True
    else:
        return False

def dbSELabBook(message):
    if(message.text==SEMand_listEdt[5]):
        return True
    else:
        return False
@bot.message_handler(func=dbSELabQuest)
def send_dbSELabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/18OGV2Z4U9OZRGB7VM-DNFFFnTWXuf-lb?usp=sharing")

@bot.message_handler(func=dbSELabBook)
def send_dbSELabBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/16g-q2c2DOKRK0x2yCe6J_m5QyfVLV3B4?usp=sharing")

#tools
SEMand_listEdt[7]="سلايدات اداوت بناء البرمجيات"
@bot.message_handler(func=ToolSE)
def send_ToolSE(message):
    ToolSEMarkup=ReplyKeyboardMarkup()
    ToolSEbtn=[]
    for i in range(7,9):
       ToolSEbtn=KeyboardButton(SEMand_listEdt[i])
       ToolSEMarkup.add(ToolSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=ToolSEMarkup)
"""def ToolSEQuest(message):
    if(message.text==SEMand_listEdt[6]):
        return True
    else:
        return False"""
def ToolSESlides(message):
    if(message.text==SEMand_listEdt[7]):
        return True
    else:
        return False
def ToolSEBook(message):
    if(message.text==SEMand_listEdt[8]):
        return True
    else:
        return False
"""@bot.message_handler(func=ToolSEQuest)
def send_ToolSEQuest(message):
    bot.send_message(message.chat.id,text="")"""
@bot.message_handler(func=ToolSESlides)
def send_ToolSESlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1LXo-UGT8xPGpU9OAWeYLHO5U-f2sCUky?usp=drive_link") 
@bot.message_handler(func=ToolSEBook)
def send_ToolSEBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1JuTj--iJyVPs_gwCIViPsUhjYS1WfLlH?usp=drive_link")

#Testing 
@bot.message_handler(func=TestSE)
def send_TestSE(message):
    TestSEMarkup=ReplyKeyboardMarkup()
    TestSEbtn=[]
    for i in range(9,12):
        if(i==10 or i==11):
            continue
        TestSEbtn=KeyboardButton(SEMand_listEdt[i])
        TestSEMarkup.add(TestSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=TestSEMarkup)
def TestSEQuest(message):
    if(message.text==SEMand_listEdt[9]):
        return True
    else:
        return False

@bot.message_handler(func=TestSEQuest)
def send_SEQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1rSIVt0V_erH_NdEMM8ipoAf0O2lO3ckl?usp=sharing")


#Android SE
@bot.message_handler(func=AndSE)
def send_AndSE(message):
    AndSEMarkup=ReplyKeyboardMarkup()
    AndSEbtn=[]
    for i in range(12,15):
       AndSEbtn=KeyboardButton(SEMand_listEdt[i])
       AndSEMarkup.add(AndSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AndSEMarkup)
def AndSEQuest(message):
    if(message.text==SEMand_listEdt[12]):
        return True
    else:
        return False
def AndSESummary(message):
    if(message.text==SEMand_listEdt[13]):
        return True
    else:
        return False
def AndSEBook(message):
    if(message.text==SEMand_listEdt[14]):
        return True
    else:
        return False
@bot.message_handler(func=AndSEQuest)
def send_AndSEQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1l6GQNARNSajxxFusqNfwILkatwTnk4Zn?usp=sharing")
@bot.message_handler(func=AndSESummary)
def send_AndSESummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1awyWBX_qXVXFLdjE-zZU2ArDk_HqGaX-?usp=sharing") 
@bot.message_handler(func=AndSEBook)
def send_AndSEBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1VnWfmrLQHSSQddIEs77j4ExWBq9ognNz?usp=sharing")

#Android SE LAB

SEMand_listEdt[17]="سلايدات مختبر البرمجة المرئية لطلبة هندسة البرمجيات"
@bot.message_handler(func=AndSElab)
def send_AndSElab(message):
    AndSElabMarkup=ReplyKeyboardMarkup()
    AndSElabbtn=[]
    for i in range(15,18):
        if(i==16):
            continue
        AndSElabbtn=KeyboardButton(SEMand_listEdt[i])
        AndSElabMarkup.add(AndSElabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AndSElabMarkup)
def AndSElabQuest(message):
    if(message.text==SEMand_listEdt[15]):
        return True
    else:
        return False

def AndSElabSlides(message):
    if(message.text==SEMand_listEdt[17]):
        return True
    else:
        return False
@bot.message_handler(func=AndSElabQuest)
def send_AndSElabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1deOBaSYwqcg6UQLzArzSV8dUkCA1Lgxb?usp=sharing")
@bot.message_handler(func=AndSElabSlides)
def send_AndSElabSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1IuZKQ4Y7kfE52kLa-YB8q0ZJ2mpktdXI?usp=sharing")


#Algorithm SE 

@bot.message_handler(func=algoSE)
def send_algoSE(message):
    algoSEMarkup=ReplyKeyboardMarkup()
    algoSEbtn=[]
    for i in range(24,27):
       algoSEbtn=KeyboardButton(SEMand_listEdt[i])
       algoSEMarkup.add(algoSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=algoSEMarkup)
def algoSEQuest(message):
    if(message.text==SEMand_listEdt[24]):
        return True
    else:
        return False
def algoSESummary(message):
    if(message.text==SEMand_listEdt[25]):
        return True
    else:
        return False
def algoSEBook(message):
    if(message.text==SEMand_listEdt[26]):
        return True
    else:
        return False
@bot.message_handler(func=algoSEQuest)
def send_algoSEQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1qJucYNbNyvQuYPpCyX_NEtDUbg4x9UT9?usp=sharing")
@bot.message_handler(func=algoSESummary)
def send_algoSESummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/13Ww5QjlAQBw2MPRnT5AqNauyGVMjKXXg?usp=sharing") 
@bot.message_handler(func=algoSEBook)
def send_algoSEBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1rF9OAak8fCVqcfEL7W6F7O_r_7gHHdMT?usp=sharing")

#Planning
SEMand_listEdt[29]="سلايدات تخطيط وادارة المشاريع"
@bot.message_handler(func=planSE)
def send_planSE(message):
    planSEMarkup=ReplyKeyboardMarkup()
    planSEbtn=[]
    for i in range(27,30):
       planSEbtn=KeyboardButton(SEMand_listEdt[i])
       planSEMarkup.add(planSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=planSEMarkup)
def planSEQuest(message):
    if(message.text==SEMand_listEdt[27]):
        return True
    else:
        return False
def planSESummary(message):
    if(message.text==SEMand_listEdt[28]):
        return True
    else:
        return False
def planSESlide(message):
    if(message.text==SEMand_listEdt[29]):
        return True
    else:
        return False
@bot.message_handler(func=planSEQuest)
def send_planSEQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1QAzSebw5HJGzl98ntzAUEDHmVOhdUNaK?usp=sharing")
@bot.message_handler(func=planSESummary)
def send_planSESummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1kCjOUOHliiH-7YUuW_jlAd7ZrsHOjIc7?usp=sharing") 
@bot.message_handler(func=planSESlide)
def send_planSESlide(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1I5-AihKNWj0pysly0shpDSPY8RsS2YdF?usp=sharing")

#designComp
@bot.message_handler(func=designCompSE)
def send_designCompSE(message):
    bot.send_message(message.chat.id,text="نعتذر لا وجود لأي شيء يتعلق بهذه المادة يرجى التواصل مع صاحب البوت في حال وجود ما لديكم يفيد في هذه المادة وشكرًا")

#User interface
@bot.message_handler(func=usrinterSE)
def send_usrinterSE(message):
    bot.send_message(message.chat.id,text="نعتذر لا وجود لأي شيء يتعلق بهذه المادة يرجى التواصل مع صاحب البوت في حال وجود ما لديكم يفيد في هذه المادة وشكرًا")



#WEB SE 
@bot.message_handler(func=webSE)
def send_webSE(message):
    webSEMarkup=ReplyKeyboardMarkup()
    webSEbtn=[]
    for i in range(36,39):
       webSEbtn=KeyboardButton(SEMand_listEdt[i])
       webSEMarkup.add(webSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=webSEMarkup)
def webSEQuest(message):
    if(message.text==SEMand_listEdt[36]):
        return True
    else:
        return False
def webSESummary(message):
    if(message.text==SEMand_listEdt[37]):
        return True
    else:
        return False
def webSEBook(message):
    if(message.text==SEMand_listEdt[38]):
        return True
    else:
        return False
@bot.message_handler(func=webSEQuest)
def send_webSEQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Hqin2jRGQ2xbqofcgnNI5c8g2Aop9QaA?usp=sharing")
@bot.message_handler(func=webSESummary)
def send_webSESummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1MHRAtNZvC6BtkO4UrOdZpoOGd9BzsB_N?usp=sharing") 
@bot.message_handler(func=webSEBook)
def send_webSEBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1A2LLw3Ur2EIBpT07FmwiTwTwz7PNUkAm?usp=sharing")

#component
SEMand_listEdt[41]="سلايدات تطوير مكونات البرمجيات"
@bot.message_handler(func=componentSE)
def send_componentSE(message):
    componentSEMarkup=ReplyKeyboardMarkup()
    componentSEbtn=[]
    for i in range(39,42):
        if(i==39 or i==40):
            continue
        componentSEbtn=KeyboardButton(SEMand_listEdt[i])
        componentSEMarkup.add(componentSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=componentSEMarkup)

def componentSEslide(message):
    if(message.text==SEMand_listEdt[41]):
        return True
    else:
        return False

@bot.message_handler(func=componentSEslide)
def send_componentSESlide(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1OLrRpknhVMCC4s38sXfrnyAIgJR0umdj?usp=sharing")

#Principle of SE 
SEMand_listEdt[45]="سلايدات مباديء هندسة البرمجيات"
SEMand_listEdt[46]="دفتر وملخص مباديء هندسة البرمجيات"
@bot.message_handler(func=principleSESE)
def send_principleSESE(message):
    principleSESEMarkup=ReplyKeyboardMarkup()
    principleSESEbtn=[]
    for i in range(45,48):
       principleSESEbtn=KeyboardButton(SEMand_listEdt[i])
       principleSESEMarkup.add(principleSESEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=principleSESEMarkup)
def principleSESESlides(message):
    if(message.text==SEMand_listEdt[45]):
        return True
    else:
        return False
def principleSESEnote(message):
    if(message.text==SEMand_listEdt[46]):
        return True
    else:
        return False
def principleSESEBook(message):
    if(message.text==SEMand_listEdt[47]):
        return True
    else:
        return False
@bot.message_handler(func=principleSESESlides)
def send_principleSESEslides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1TAkOFsPju0ebeZJwDXXTcsUtF4NGxWpN?usp=sharing")
@bot.message_handler(func=principleSESEnote)
def send_principleSESEnote(message):
    bot.send_message(message.chat.id,text="دفتر : https://drive.google.com/drive/folders/1drrlSXfkqTzazv8va7woILginNs9mW9a?usp=sharing")
    bot.send_message(message.chat.id,text="ملخص : https://drive.google.com/drive/folders/193JvZafCwFS4P_inXPhAkLkflvOXlNYU?usp=sharing")
@bot.message_handler(func=principleSESEBook)
def send_principleSESEBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1s8fNLWrOIH2o9-FPh5NT-3a-ZcN2FrK9?usp=drive_link")


#DS SE 

SEMand_listEdt[49]="سلايدات البرامج والملفات والبيانات"
@bot.message_handler(func=DSSE)
def send_DSSE(message):
    DSSEMarkup=ReplyKeyboardMarkup()
    DSSEbtn=[]
    for i in range(48,51):
       DSSEbtn=KeyboardButton(SEMand_listEdt[i])
       DSSEMarkup.add(DSSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=DSSEMarkup)
def DSSEQuest(message):
    if(message.text==SEMand_listEdt[48]):
        return True
    else:
        return False
def DSSEslides(message):
    if(message.text==SEMand_listEdt[49]):
        return True
    else:
        return False
def DSSEBook(message):
    if(message.text==SEMand_listEdt[50]):
        return True
    else:
        return False
@bot.message_handler(func=DSSEslides)
def send_DSSEslides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1M8vOv68NqPxo9tWunn82xfSc3JwMG4Gz?usp=drive_link")
@bot.message_handler(func=DSSEQuest)
def send_DSSEQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1zpEaFyYo5l6d8E4NKmnP1ixJixJxLAcb?usp=drive_link") 
@bot.message_handler(func=DSSEBook)
def send_DSSEBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1KPFM3tD7AcZ9H-Gaq8C51DH_AWQCxqqM?usp=sharing")

#DS SE LAB

SEMand_listEdt[53]="سلايدات مختبر هيكلة البرامج والملفات و البيانات"
@bot.message_handler(func=DSSElab)
def send_DSSElab(message):
    DSSElabMarkup=ReplyKeyboardMarkup()
    DSSElabbtn=[]
    for i in range(51,54):
        if(i==52):
            continue
        DSSElabbtn=KeyboardButton(SEMand_listEdt[i])
        DSSElabMarkup.add(DSSElabbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=DSSElabMarkup)
def DSSElabQuest(message):
    if(message.text==SEMand_listEdt[51]):
        return True
    else:
        return False

def DSSElabSlides(message):
    if(message.text==SEMand_listEdt[53]):
        return True
    else:
        return False
@bot.message_handler(func=DSSElabQuest)
def send_DSSElabQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1X7RQbrClMj0TkczfaoYbhhKJ_0k47YfS?usp=sharing")

@bot.message_handler(func=DSSElabSlides)
def send_DSSElabSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/14BdLdtkwva4EvIYExEXh4NfO7tJo7yPT?usp=sharing")

#OS SE 
@bot.message_handler(func=OSSE)
def send_OSSE(message):
    OSSEMarkup=ReplyKeyboardMarkup()
    OSSEbtn=[]
    for i in range(54,57):
       OSSEbtn=KeyboardButton(SEMand_listEdt[i])
       OSSEMarkup.add(OSSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=OSSEMarkup)
def OSSEQuest(message):
    if(message.text==SEMand_listEdt[54]):
        return True
    else:
        return False
def OSSESummary(message):
    if(message.text==SEMand_listEdt[55]):
        return True
    else:
        return False
def OSSEBook(message):
    if(message.text==SEMand_listEdt[56]):
        return True
    else:
        return False
@bot.message_handler(func=OSSEQuest)
def send_OSSEQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Jowd4a4CjGLGiGplfrTlHaC-zPaRTPJD?usp=sharing")
@bot.message_handler(func=OSSESummary)
def send_OSSESummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/14kOC1LaCOpN6cYHzFLecmgZCzcxvZC_k?usp=sharing") 
@bot.message_handler(func=OSSEBook)
def send_OSSEBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1jgiGnfHy9ltax2aMS7yLMx9XblmFowOf?usp=sharing")

#شيئية
@bot.message_handler(func=objectSE)
def send_objectSE(message):
    objectSEMarkup=ReplyKeyboardMarkup()
    objectSEbtn=[]
    for i in range(57,60):
       objectSEbtn=KeyboardButton(SEMand_listEdt[i])
       objectSEMarkup.add(objectSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=objectSEMarkup)
def objectSEQuest(message):
    if(message.text==SEMand_listEdt[57]):
        return True
    else:
        return False
def objectSESummary(message):
    if(message.text==SEMand_listEdt[58]):
        return True
    else:
        return False
def objectSEBook(message):
    if(message.text==SEMand_listEdt[59]):
        return True
    else:
        return False
@bot.message_handler(func=objectSEQuest)
def send_objectSEQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1DPsr4LzQBiryV_Ld6oi_20Tp6KE7HpR5?usp=sharing")
@bot.message_handler(func=objectSESummary)
def send_objectSESummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1ZAcR-uXW2PwCMssTXb_1yA81sALoAOVk?usp=sharing") 
@bot.message_handler(func=objectSEBook)
def send_objectSEBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1aGDWzURmrXyX4c56mdJ094buOmX-jae6?usp=sharing")

#Requirement SE
SEMand_listEdt[60]="سلايدات هندسة ووصف متطلبات البرمجيات"
SEMand_listEdt[61]="دفتر زملخص هندسة ووصف متطلبات البرمجيات"
@bot.message_handler(func=reqSE)
def send_reqSE(message):
    reqSEMarkup=ReplyKeyboardMarkup()
    reqSEbtn=[]
    for i in range(60,63):
       reqSEbtn=KeyboardButton(SEMand_listEdt[i])
       reqSEMarkup.add(reqSEbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=reqSEMarkup)
def reqSESlides(message):
    if(message.text==SEMand_listEdt[60]):
        return True
    else:
        return False
def reqSEnote(message):
    if(message.text==SEMand_listEdt[61]):
        return True
    else:
        return False
def reqSEBook(message):
    if(message.text==SEMand_listEdt[62]):
        return True
    else:
        return False
@bot.message_handler(func=reqSESlides)
def send_reqSESlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1KtaSvwq1r11Ilvbmd3oFCAh56yGifNd8?usp=drive_link")
@bot.message_handler(func=reqSEnote)
def send_reqSESummary(message):
    bot.send_message(message.chat.id,text="دفتر : https://drive.google.com/drive/folders/1vf8YszaQ9iaX93SozCWSHz1oyWb0-RLO?usp=drive_link")
    bot.send_message(message.chat.id,text="ملخص :https://drive.google.com/drive/folders/1Yep-u_wjf8xdRm9Y92UVyaQAP_xPPItt?usp=drive_link") 
@bot.message_handler(func=reqSEBook)
def send_reqSEBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1aOISRgGGIf93UwP3fKMrWmhQrO3Fs41T?usp=drive_link")


########################
#####CGA Mandatory######
########################
@bot.message_handler(func=CGA)
def send_CGAmand(message):
    CGAMandMarkup=ReplyKeyboardMarkup()
    CGAMandbtn=[]
    for i in range(len(CGAMandlist)):
        if(i%2==0 and i<len(CGAMandlist)-1):
            CGAMandbtn=KeyboardButton(CGAMandlist[i])
            CGAMandbtn2=KeyboardButton(CGAMandlist[i+1])
            CGAMandMarkup.add(CGAMandbtn,CGAMandbtn2)
        else:
            CGAMandbtn=KeyboardButton(CGAMandlist[i])
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=CGAMandMarkup)
def MathCGA(message):
    if(message.text==CGAMandlist[3]):
        return True
    else:
        return False
def principleCGA(message):
    if(message.text==CGAMandlist[4]):
        return True
    else:
        return False
def colorCGA(message):
    if(message.text==CGAMandlist[5]):
        return True
    else:
        return False
def a2dCGA(message):
    if(message.text==CGAMandlist[6]):
        return True
    else:
        return False
def lab2dCGA(message):
    if(message.text==CGAMandlist[7]):
        return True
    else:
        return False
def ArtCGA(message):
    if(message.text==CGAMandlist[8]):
        return True
    else:
        return False
def AndCGA(message):
    if(message.text==CGAMandlist[9]):
        return True
    else:
        return False
def intGraphCGA(message):
    if(message.text==CGAMandlist[12]):
        return True
    else:
        return False
def comicCGA(message):
    if(message.text==CGAMandlist[13]):
        return True
    else:
        return False
def a3dCGA(message):
    if(message.text==CGAMandlist[14]):
        return True
    else:
        return False
def charCGA(message):
    if(message.text==CGAMandlist[15]):
        return True
    else:
        return False
def a3dAnimCGA(message):
    if(message.text==CGAMandlist[17]):
        return True
    else:
        return False
def HCICGA(message):
    if(message.text==CGAMandlist[18]):
        return True
    else:
        return False
def filmCGA(message):
    if(message.text==CGAMandlist[19]):
        return True
    else:
        return False
def filmLabCGA(message):
    if(message.text==CGAMandlist[20]):
        return True
    else:
        return False
def beautyCGA(message):
    if(message.text==CGAMandlist[21]):
        return True
    else:
        return False


#الرياضيات للرسم الحاسوبي
CGAMandlistEdt[10]="دفتر الرياضيات للرسم الحاسوبي" 
@bot.message_handler(func=MathCGA)
def send_MathCGA(message):
    MathCGAMarkup=ReplyKeyboardMarkup()
    MathCGAbtn=[]
    for i in range(9,11):
       MathCGAbtn=KeyboardButton(CGAMandlistEdt[i])
       MathCGAMarkup.add(MathCGAbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=MathCGAMarkup)
def MathCGAQuest(message):
    if(message.text==CGAMandlistEdt[9]):
        return True
    else:
        return False
def MathCGANote(message):
    if(message.text==CGAMandlistEdt[10]):
        return True
    else:
        return False

@bot.message_handler(func=MathCGAQuest)
def send_MathCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1-L3IiIAx7RVJ288HtFk3nuSvSC6uhZfP?usp=drive_link")
@bot.message_handler(func=MathCGANote)
def send_MathCGANote(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1r6pk2hS5Y6bpi_hnWbFzE2tRFljdmEx1?usp=drive_link")
#مبادئ الرسم الحاسوبي
@bot.message_handler(func=principleCGA)
def send_principleCGA(message):
    principleCGAMarkup=ReplyKeyboardMarkup()
    principleCGAbtn=[]
    for i in range(12,15):
       principleCGAbtn=KeyboardButton(CGAMandlistEdt[i])
       principleCGAMarkup.add(principleCGAbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=principleCGAMarkup)
def principleCGAQuest(message):
    if(message.text==CGAMandlistEdt[12]):
        return True
    else:
        return False
def principleCGASummary(message):
    if(message.text==CGAMandlistEdt[13]):
        return True
    else:
        return False
def principleCGABook(message):
    if(message.text==CGAMandlistEdt[14]):
        return True
    else:
        return False

@bot.message_handler(func=principleCGAQuest)
def send_principleCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1QX-bSuRseFcngS59Zg020GrfOjqIppY_?usp=drive_link")
@bot.message_handler(func=principleCGASummary)
def send_principleCGASummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1-1Nl5Gr13uYWTPlHqGZBKXCtUdZbjlLk?usp=drive_link") 
@bot.message_handler(func=principleCGABook)
def send_principleCGABook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1dKie6BCe6ULtS2phhurr3hIGPqaWYe3m?usp=drive_link")

#طرق التلوين
CGAMandlistEdt[16]="سلايدات طرق التلوين"
CGAMandlistEdt[17]="واجبات طرق التلوين"
@bot.message_handler(func=colorCGA)
def send_colorCGA(message):
    CGAcolorMarkup=ReplyKeyboardMarkup()
    CGAcolorbtn=[]
    for i in range(15,18):
       CGAcolorbtn=KeyboardButton(CGAMandlistEdt[i])
       CGAcolorMarkup.add(CGAcolorbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=CGAcolorMarkup)
def CGAcolorQuest(message):
    if(message.text==CGAMandlistEdt[15]):
        return True
    else:
        return False
def CGAcolorSlides(message):
    if(message.text==CGAMandlistEdt[16]):
        return True
    else:
        return False
def CGAcolorAss(message):
    if(message.text==CGAMandlistEdt[17]):
        return True
    else:
        return False
@bot.message_handler(func=CGAcolorQuest)
def send_CGAcolorQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1CHFF0OfZhQzX6uiDIWYndDGjnzgD7p6b?usp=drive_link")
@bot.message_handler(func=CGAcolorSlides)
def send_CGAcolorSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1MrozeQT-ZLBVCZ13t1H58vqbtxlodZqb?usp=drive_link") 
@bot.message_handler(func=CGAcolorAss)
def send_CGAcolorAss(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1K23GVdbziFTnU-N2_lCAnHyJbXRJUGaQ?usp=drive_link")

    
#2D Animation رسوم متحركة ثنائية الابعاد
CGAMandlistEdt[19]="سلايدات الرسوم المتحركة ثنائية الابعاد"
@bot.message_handler(func=a2dCGA)
def send_a2dCGA(message):
    a2dCGAMarkup=ReplyKeyboardMarkup()
    a2dCGAbtn=[]
    for i in range(18,20):
       a2dCGAbtn=KeyboardButton(CGAMandlistEdt[i])
       a2dCGAMarkup.add(a2dCGAbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=a2dCGAMarkup)
def a2dCGAQuest(message):
    if(message.text==CGAMandlistEdt[18]):
        return True
    else:
        return False
def a2dCGASlides(message):
    if(message.text==CGAMandlistEdt[19]):
        return True
    else:
        return False
@bot.message_handler(func=a2dCGAQuest)
def send_a2dCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1hj65fiIZODtO4aPzL1SXOujviKtSmDtP?usp=sharing")
@bot.message_handler(func=a2dCGASlides)
def send_a2dCGASlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Ef0QbvOeYx3MaDxAN3_Q8-7eWilY7kxd?usp=drive_link") 

#2D Animation Lab
@bot.message_handler(func=lab2dCGA)
def send_lab2dCGA(message):
    lab2dCGAMarkup=ReplyKeyboardMarkup()
    lab2dCGAMarkup.add(CGAMandlistEdt[21])
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=lab2dCGAMarkup)
def lab2dCGAQuest(message):
    if(message.text==CGAMandlistEdt[21]):
        return True
    else:
        return False

@bot.message_handler(func=lab2dCGAQuest)
def send_lab2dCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1HBInEpHIQ2gMUv4fzywGK7rAhB_yyfBY?usp=drive_link")

#تطبيقات الحاسوب في الفنون
@bot.message_handler(func=ArtCGA)
def send_ArtCGA(message):
    ArtCGAMarkup=ReplyKeyboardMarkup()
    ArtCGAbtn=[]
    for i in range(24,27):
        if (i==25):
            continue
        ArtCGAbtn=KeyboardButton(CGAMandlistEdt[i])
        ArtCGAMarkup.add(ArtCGAbtn)   
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=ArtCGAMarkup)
def ArtCGAQuest(message):
    if(message.text==CGAMandlistEdt[24]):
        return True
    else:
        return False
def ArtCGABook(message):
    if(message.text==CGAMandlistEdt[26]):
        return True
    else:
        return False

@bot.message_handler(func=ArtCGAQuest)
def send_ArtCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1YixqNpMOe-vT3Puw26_r1EbLeBhxUK81?usp=drive_link")
@bot.message_handler(func=ArtCGABook)
def send_ArtCGABook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1q9zOM3n2vjpMqL5xnmZdobU95rAydsr7?usp=drive_link")


#Android CGA
@bot.message_handler(func=AndCGA)
def send_AndCGA(message):
    AndCGAMarkup=ReplyKeyboardMarkup()
    AndCGAbtn=[]
    for i in range(27,30):
       AndCGAbtn=KeyboardButton(CGAMandlistEdt[i])
       AndCGAMarkup.add(AndCGAbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AndCGAMarkup)
def AndCGAQuest(message):
    if(message.text==CGAMandlistEdt[27]):
        return True
    else:
        return False
def AndCGASummary(message):
    if(message.text==CGAMandlistEdt[28]):
        return True
    else:
        return False
def AndCGABook(message):
    if(message.text==CGAMandlistEdt[29]):
        return True
    else:
        return False
@bot.message_handler(func=AndCGAQuest)
def send_AndCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1l6GQNARNSajxxFusqNfwILkatwTnk4Zn?usp=sharing")
@bot.message_handler(func=AndCGASummary)
def send_AndCGASummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1awyWBX_qXVXFLdjE-zZU2ArDk_HqGaX-?usp=sharing") 
@bot.message_handler(func=AndCGABook)
def send_AndCGABook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1VnWfmrLQHSSQddIEs77j4ExWBq9ognNz?usp=sharing")

#Interactive Graphics الرسومات التفاعلية
@bot.message_handler(func=intGraphCGA)
def send_intGraphCGA(message):
    intGraphCGAMarkup=ReplyKeyboardMarkup()
    intGraphCGAMarkup.add(CGAMandlistEdt[36])
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=intGraphCGAMarkup)
def intGraphCGAQuest(message):
    if(message.text==CGAMandlistEdt[36]):
        return True
    else:
        return False

@bot.message_handler(func=intGraphCGAQuest)
def send_intGraphCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1dyvJeshCfG9sLfX7taMhzwzQ89uG-gHE?usp=drive_link")

#تصميم القصة المصورة
CGAMandlistEdt[40]="سلايدات تصميم القصة المصورة"
@bot.message_handler(func=comicCGA)
def send_comicCGA(message):
    comicCGAMarkup=ReplyKeyboardMarkup()
    comicCGAbtn=[]
    for i in range(39,42):
       comicCGAbtn=KeyboardButton(CGAMandlistEdt[i])
       comicCGAMarkup.add(comicCGAbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=comicCGAMarkup)
def comicCGAQuest(message):
    if(message.text==CGAMandlistEdt[39]):
        return True
    else:
        return False
def comicCGASummary(message):
    if(message.text==CGAMandlistEdt[40]):
        return True
    else:
        return False
def comicCGABook(message):
    if(message.text==CGAMandlistEdt[41]):
        return True
    else:
        return False
@bot.message_handler(func=comicCGAQuest)
def send_comicCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1NFqqz13lAQm6xWfS-hpbvIardFIgRHSs?usp=drive_link")
@bot.message_handler(func=comicCGASummary)
def send_comicCGASummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1uK-kJws4rtjBniACMFohXKWLPpROcQfY?usp=drive_link") 
@bot.message_handler(func=comicCGABook)
def send_comicCGABook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1Eegl_uQSU0kF5o4nIZDuhrhPAg76FTht?usp=drive_link")


#تصميم النماذج ثلاثية الابعاد
CGAMandlistEdt[43]="سلايدات تصميم النماذج ثلاثية الابعاد"
CGAMandlistEdt[44]="واجبات تصميم النماذج ثلاثية الابعاد"
@bot.message_handler(func=a3dCGA)
def send_a3dCGA(message):
    a3dCGAMarkup=ReplyKeyboardMarkup()
    a3dCGAbtn=[]
    for i in range(42,45):
       a3dCGAbtn=KeyboardButton(CGAMandlistEdt[i])
       a3dCGAMarkup.add(a3dCGAbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=a3dCGAMarkup)
def a3dCGAQuest(message):
    if(message.text==CGAMandlistEdt[42]):
        return True
    else:
        return False
def a3dCGASlides(message):
    if(message.text==CGAMandlistEdt[43]):
        return True
    else:
        return False
def a3dCGAAss(message):
    if(message.text==CGAMandlistEdt[44]):
        return True
    else:
        return False
@bot.message_handler(func=a3dCGAQuest)
def send_a3dCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1fEwftEoAMo-TnHRhW7O6i4Fo3SM8U7F6?usp=drive_link")
@bot.message_handler(func=a3dCGASlides)
def send_a3dCGASlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1pJPZkGuehgc-Ey1KhUvD6LXScGrF8Tqj?usp=drive_link") 
@bot.message_handler(func=a3dCGAAss)
def send_a3dCGAAss(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1QdW-o4_mZHQA8PpcJwo9F9G7pINyIQNu?usp=drive_link")


#Character تصميم الشخصيات ثلاثية الابعاد 
@bot.message_handler(func=charCGA)
def send_charCGA(message):
    bot.send_message(message.chat.id,text="نأسف لعدم وجود ما يفيد في هذه المادة")

#تحريك الشخصيات ثلاثية الابعاد
@bot.message_handler(func=a3dAnimCGA)
def send_a3dAnimCGA(message):
    a3dAnimCGAMarkup=ReplyKeyboardMarkup()
    a3dAnimCGAMarkup.add(CGAMandlistEdt[51])
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=a3dAnimCGAMarkup)
def a3dAnimCGAQuest(message):
    if(message.text==CGAMandlistEdt[51]):
        return True
    else:
        return False
@bot.message_handler(func=a3dAnimCGAQuest)
def send_a3dAnimCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1RVQ8THhfRsJb1Nz5fqRqDFkd1W5-Dgrp?usp=drive_link")


#Human Computer Interaction تفاعل الانسان والحاسب الآلي
@bot.message_handler(func=HCICGA)
def send_HCICGA(message):
    HCICGAMarkup=ReplyKeyboardMarkup()
    HCICGAbtn=[]
    for i in range(54,57):
       HCICGAbtn=KeyboardButton(CGAMandlistEdt[i])
       HCICGAMarkup.add(HCICGAbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=HCICGAMarkup)
def HCICGAQuest(message):
    if(message.text==CGAMandlistEdt[54]):
        return True
    else:
        return False
def HCICGASummary(message):
    if(message.text==CGAMandlistEdt[55]):
        return True
    else:
        return False
def HCICGABook(message):
    if(message.text==CGAMandlistEdt[56]):
        return True
    else:
        return False

@bot.message_handler(func=HCICGAQuest)
def send_HCICGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1c4ZvNY9XSIBHCN1U74KfQ1jENqqW3jnX?usp=drive_link")
@bot.message_handler(func=HCICGASummary)
def send_HCICGASummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1v6WX5ujFiRd1Kh_xsgVNMMgN_FVDGwWA?usp=drive_link") 
@bot.message_handler(func=HCICGABook)
def send_HCICGABook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1sGQBYSo8F1ZYpmUWoWdhjYLCc5IO-ftW?usp=drive_link")


#تصميم الافلام الرقمية
CGAMandlistEdt[59]="سلايدات تصميم الافلام الرقمية"
@bot.message_handler(func=filmCGA)
def send_filmCGA(message):
    filmCGAMarkup=ReplyKeyboardMarkup()
    filmCGAbtn=[]
    for i in range(57,60):
       filmCGAbtn=KeyboardButton(CGAMandlistEdt[i])
       filmCGAMarkup.add(filmCGAbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=filmCGAMarkup)
def filmCGAQuest(message):
    if(message.text==CGAMandlistEdt[57]):
        return True
    else:
        return False
def filmCGASummary(message):
    if(message.text==CGAMandlistEdt[58]):
        return True
    else:
        return False
def filmCGASlides(message):
    if(message.text==CGAMandlistEdt[59]):
        return True
    else:
        return False
@bot.message_handler(func=filmCGAQuest)
def send_filmCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1qDQmPdWW1jaIRYOZI1wetzZDpNBck9EK?usp=drive_link")
@bot.message_handler(func=filmCGASummary)
def send_filmCGASummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1XvgLvxOiYhW4Wj_xaTa_ir_yAU5EWi6E?usp=drive_link") 
@bot.message_handler(func=filmCGASlides)
def send_filmCGASlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1APSFPxouckxrb9_qqk6J1A0eoiSDhAx8?usp=drive_link")

#مختبر تصميم الافلام الرقمية
@bot.message_handler(func=filmLabCGA)
def send_filmLabCGA(message):
    filmLabCGAMarkup=ReplyKeyboardMarkup()
    filmLabCGAMarkup.add(CGAMandlistEdt[60])
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=filmLabCGAMarkup)
def filmLabCGAQuest(message):
    if(message.text==CGAMandlistEdt[60]):
        return True
    else:
        return False

@bot.message_handler(func=filmLabCGAQuest)
def send_filmLabCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/14CPnH70zXhbGfP-IWUc4Az0LT2_8JuU6?usp=sharing")
  
#مبادئ الفنون الجميلة
@bot.message_handler(func=beautyCGA)
def send_beautyCGA(message):
    beautyCGAMarkup=ReplyKeyboardMarkup()
    beautyCGAMarkup.add(CGAMandlistEdt[63])
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=beautyCGAMarkup)
def beautyCGAQuest(message):
    if(message.text==CGAMandlistEdt[63]):
        return True
    else:
        return False

@bot.message_handler(func=beautyCGAQuest)
def send_beautyCGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1kSw9rLbMQGhJKtS2sDSzYTnUyYi32pEQ?usp=drive_link")

#متطلب تخصص اختياري
@bot.message_handler(func=majorOpt)
def send_majorOpt(message):
    majorOptmarkup=ReplyKeyboardMarkup()
    majorOptbtn=[]
    for i in range(len(majorOptlist)):
        majorOptbtn=KeyboardButton(majorOptlist[i])
        majorOptmarkup.add(majorOptbtn)
    bot.send_message(message.chat.id,text="اختر التخصص",reply_markup=majorOptmarkup)
def CSopt(message):
    if(message.text==majorOptlist[0]):
        return True
    else:
        return False
def CISopt(message):
    if(message.text==majorOptlist[1]):
        return True
    else:
        return False
def CGAopt(message):
    if(message.text==majorOptlist[2]):
        return True
    else:
        return False
    



# CS Optional
@bot.message_handler(func=CSopt)
def send_CSopt(message):
    CSoptMarkup=ReplyKeyboardMarkup()
    CSoptbtn=[]
    for i in range(len(CSOptlist)):
        CSoptbtn=KeyboardButton(CSOptlist[i])
        CSoptMarkup.add(CSoptbtn)
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=CSoptMarkup)
def MultiCSopt(message):
    if(message.text==CSOptlist[0]):
        return True
    else:
        return False
def CloudCSopt(message):
    if(message.text==CSOptlist[1]):
        return True
    else:
        return False
def ImgCSopt(message):
    if(message.text==CSOptlist[2]):
        return True
    else:
        return False
def TopicsCSopt(message):
    if(message.text==CSOptlist[3]):
        return True
    else:
        return False
def AICSopt(message):
    if(message.text==CSOptlist[4]):
        return True
    else:
        return False

    
#MultiMedia CS 
@bot.message_handler(func=MultiCSopt)
def send_MultiCS(message):
    MultiCSoptMarkup=ReplyKeyboardMarkup()
    MultiCSoptbtn=[]
    for i in range(3):
       MultiCSoptbtn=KeyboardButton(CSOptlistEdt[i])
       MultiCSoptMarkup.add(MultiCSoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=MultiCSoptMarkup)
def MultiCSoptQuest(message):
    if(message.text==CSOptlistEdt[0]):
        return True
    else:
        return False
def MultiCSoptSummary(message):
    if(message.text==CSOptlistEdt[1]):
        return True
    else:
        return False
def MultiCSoptBook(message):
    if(message.text==CSOptlistEdt[2]):
        return True
    else:
        return False
@bot.message_handler(func=MultiCSoptQuest)
def send_MultiCSoptQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1VRktB5PWwH3SRpfE472xPP_mCEIFKShB?usp=drive_link")
@bot.message_handler(func=MultiCSoptSummary)
def send_MultiCSoptSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/10e5HGC_6cbnQNiSJ9Wu4XkP7gc9O4vjN?usp=drive_link") 
@bot.message_handler(func=MultiCSoptBook)
def send_MultiCSoptBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/18OPANwUSvXSFS7pAeXTqDg40afh9hlpZ?usp=drive_link")

#Cloud Computing CS 
@bot.message_handler(func=CloudCSopt)
def send_CloudCS(message):
    CloudCSoptMarkup=ReplyKeyboardMarkup()
    CloudCSoptbtn=[]
    for i in range(3,6):
        if(i==4):
            continue
        CloudCSoptbtn=KeyboardButton(CSOptlistEdt[i])
        CloudCSoptMarkup.add(CloudCSoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=CloudCSoptMarkup)
def CloudCSoptQuest(message):
    if(message.text==CSOptlistEdt[3]):
        return True
    else:
        return False
def CloudCSoptBook(message):
    if(message.text==CSOptlistEdt[5]):
        return True
    else:
        return False
@bot.message_handler(func=CloudCSoptQuest)
def send_CloudCSoptQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1f13Bt99taPkAsG396dWp3QAxSfGgP0m7?usp=share_link")

@bot.message_handler(func=CloudCSoptBook)
def send_CloudCSoptBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1YIitnsqQRwXZSz7W3iWeY10LhBZrWQ4W?usp=share_link")

#Image processing CS
CSOptlistEdt[7]="سلايدات معالجة الصور والرؤيا الرقمية"
@bot.message_handler(func=ImgCSopt)
def send_ImgCS(message):
    ImgCSoptMarkup=ReplyKeyboardMarkup()
    ImgCSoptbtn=[]
    for i in range(6,9):
       ImgCSoptbtn=KeyboardButton(CSOptlistEdt[i])
       ImgCSoptMarkup.add(ImgCSoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=ImgCSoptMarkup)
def ImgCSoptQuest(message):
    if(message.text==CSOptlistEdt[6]):
        return True
    else:
        return False
def ImgCSoptSlides(message):
    if(message.text==CSOptlistEdt[7]):
        return True
    else:
        return False
def ImgCSoptBook(message):
    if(message.text==CSOptlistEdt[8]):
        return True
    else:
        return False
@bot.message_handler(func=ImgCSoptQuest)
def send_ImgCSoptQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1yyo8Dv7k5pUoHV2NWIyXXoPIqm5XUUE8?usp=share_link")
@bot.message_handler(func=ImgCSoptSlides)
def send_ImgCSoptSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1z6AMh_BUVGEJHzOTUasYjbi_Jj5SE-C5?usp=share_link") 
@bot.message_handler(func=ImgCSoptBook)
def send_ImgCSoptBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1mjySJbLlh_MVccA242VMwnecb5zQ9noC?usp=share_link")

#Topics CS 
@bot.message_handler(func=TopicsCSopt)
def send_TopicsCS(message):
    TopicsCSoptMarkup=ReplyKeyboardMarkup()
    TopicsCSoptbtn=[]
    for i in range(9,12):
        if(i==10):
            continue
        TopicsCSoptbtn=KeyboardButton(CSOptlistEdt[i])
        TopicsCSoptMarkup.add(TopicsCSoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=TopicsCSoptMarkup)
def TopicsCSoptQuest(message):
    if(message.text==CSOptlistEdt[9]):
        return True
    else:
        return False

def TopicsCSoptBook(message):
    if(message.text==CSOptlistEdt[11]):
        return True
    else:
        return False
@bot.message_handler(func=TopicsCSoptQuest)
def send_TopicsCSoptQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1GpvVdX44OcSfxT-qeQX0hOo0mUt3u2EL?usp=share_link")

@bot.message_handler(func=TopicsCSoptBook)
def send_TopicsCSoptBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1x6lYomcZE2Oj-OGG37zPwxCZZ1voknGG?usp=share_link")


#AI OPTIONAL CS 
CSOptlistEdt[13]="سلايدات الذكاء الاصطناعي المتقدم والتعلم الآلي"
@bot.message_handler(func=AICSopt)
def send_AICSopt(message):
    AICSoptMarkup=ReplyKeyboardMarkup()
    AICSoptbtn=[]
    for i in range(12,14):
       AICSoptbtn=KeyboardButton(CSOptlistEdt[i])
       AICSoptMarkup.add(AICSoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=AICSoptMarkup)
def AICSoptQuest(message):
    if(message.text==CSOptlistEdt[12]):
        return True
    else:
        return False
def AICSoptSlides(message):
    if(message.text==CSOptlistEdt[13]):
        return True
    else:
        return False

@bot.message_handler(func=AICSoptQuest)
def send_AICSoptQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1dC4VDp7CGXseQLIQpfnXrGDE4VsSg4w-?usp=share_link")
@bot.message_handler(func=AICSoptSlides)
def send_AICSoptSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1JxY-H1jcuVRapUxjVJCM9m5HM1YE_tIP?usp=share_link") 


#CIS Optional
@bot.message_handler(func=CISopt)
def send_CISopt(message):
    CISoptMarkup=ReplyKeyboardMarkup()
    CISoptbtn=[]
    for i in range(len(CISOptlist)):
        CISoptbtn=KeyboardButton(CISOptlist[i])
        CISoptMarkup.add(CISoptbtn)
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=CISoptMarkup)

def ClusCISopt(message):
    if(message.text==CISOptlist[0]):
        return True
    else:
        return False
def GISCISopt(message):
    if(message.text==CISOptlist[1]):
        return True
    else:
        return False
def EcommerceCISopt(message):
    if(message.text==CISOptlist[2]):
        return True
    else:
        return False

#E-commerce 
CISOptlistEdt[7]="سلايدات انظمة التجارة الالكترونية"
@bot.message_handler(func=EcommerceCISopt)
def send_EcommerceCISopt(message):
    EcommerceCISoptMarkup=ReplyKeyboardMarkup()
    EcommerceCISoptbtn=[]
    for i in range(7,9):
       EcommerceCISoptbtn=KeyboardButton(CISOptlistEdt[i])
       EcommerceCISoptMarkup.add(EcommerceCISoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=EcommerceCISoptMarkup)
"""def EcommerceCISoptQuest(message):
    if(message.text==CISOptlistEdt[6]):
        return True
    else:
        return False"""
def EcommerceCISoptSlides(message):
    if(message.text==CISOptlistEdt[7]):
        return True
    else:
        return False
def EcommerceCISoptBook(message):
    if(message.text==CISOptlistEdt[8]):
        return True
    else:
        return False
"""@bot.message_handler(func=EcommerceCISoptQuest)
def send_EcommerceCISoptQuest(message):
    bot.send_message(message.chat.id,text="")"""
@bot.message_handler(func=EcommerceCISoptSlides)
def send_EcommerceCISoptSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1WEbsOpgA7X232UWhXtSqW3ERv-cQmH5O?usp=drive_link") 
@bot.message_handler(func=EcommerceCISoptBook)
def send_EcommerceCISoptBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1CEO6UhKXi8aKsgYO82BXqxfvOiqKq6Yv?usp=drive_link")

#CIS pattern classification and clustering
CISOptlistEdt[0]="سلايدات تصنيف وتقييم الانماط"
CISOptlistEdt[1]="سلايدات تصنيف وتقييم الانماط مع ملاحظات"
CISOptlistEdt[2]="ملفات CSV"
@bot.message_handler(func=ClusCISopt)
def send_ClusCISopt(message):
    ClusCISoptMarkup=ReplyKeyboardMarkup()
    ClusCISoptbtn=[]
    for i in range(3):
       ClusCISoptbtn=KeyboardButton(CISOptlistEdt[i])
       ClusCISoptMarkup.add(ClusCISoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=ClusCISoptMarkup)
def ClusCISoptSlides(message):
    if(message.text==CISOptlistEdt[0]):
        return True
    else:
        return False
def ClusCISoptSlidesRyalat(message):
    if(message.text==CISOptlistEdt[1]):
        return True
    else:
        return False
def ClusCsv(message):
    if(message.text==CISOptlistEdt[2]):
        return True
    else:
        return False
@bot.message_handler(func=ClusCISoptSlides)
def send_ClusCISoptSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1XihLG_wvclM0OuSBPCELE5mUT0-nY2fN?usp=share_link")
@bot.message_handler(func=ClusCISoptSlidesRyalat)
def send_ClusCISoptSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1gcU1P7kegh9Nlk09tpxiLJJ7Fn8tBWDX?usp=sharing") 
@bot.message_handler(func=ClusCsv)
def send_ClusCISoptBook(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1r4pJCfL-hvfuIECTUIbzJ2zyyheFbVVh?usp=sharing")

#GIS CIS 
@bot.message_handler(func=GISCISopt)
def send_GISCISopt(message):
    GISCISoptMarkup=ReplyKeyboardMarkup()
    GISCISoptbtn=KeyboardButton(CISOptlistEdt[3])
    GISCISoptMarkup.add(GISCISoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=GISCISoptMarkup)
def GISCISoptQuest(message):
    if(message.text==CISOptlistEdt[3]):
        return True
    else:
        return False
@bot.message_handler(func=GISCISoptQuest)
def send_GISCISoptQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1R3Ie-lPxGK1SvT1YOL1S3eBeOAGzUqWi?usp=share_link")

#CGA Optional
@bot.message_handler(func=CGAopt)
def send_CGAopt(message):
    CGAoptMarkup=ReplyKeyboardMarkup()
    CGAoptbtn=[]
    for i in range(len(CGAOptlist)):
        CGAoptbtn=KeyboardButton(CGAOptlist[i])
        CGAoptMarkup.add(CGAoptbtn)
    bot.send_message(message.chat.id,text="اختر المادة",reply_markup=CGAoptMarkup)

def ComputCGAopt(message):
    if(message.text==CGAOptlist[0]):
        return True
    else:
        return False
def GameCGAopt(message):
    if(message.text==CGAOptlist[2]):
        return True
    else:
        return False
def TopicCGAopt(message):
    if(message.text==CGAOptlist[3]):
        return True
    else:
        return False
def GameProgCGAopt(message):
    if(message.text==CGAOptlist[4]):
        return True
    else:
        return False





#Computing الحوسبة المتوازية
@bot.message_handler(func=ComputCGAopt)
def send_ComputCGA(message):
    ComputCGAoptMarkup=ReplyKeyboardMarkup()
    ComputCGAoptMarkup.add(CGAOptlistEdt[0])
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=ComputCGAoptMarkup)
def ComputCGAoptQuest(message):
    if(message.text==CGAOptlistEdt[0]):
        return True
    else:
        return False

@bot.message_handler(func=ComputCGAoptQuest)
def send_ComputCGAoptQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1CT9MTzHtRNDOsONEXMJC5Ir8S_AKtzWu?usp=drive_link")

#Games تصميم الالعاب
@bot.message_handler(func=GameCGAopt)
def send_GameCGA(message):
    GameCGAoptMarkup=ReplyKeyboardMarkup()
    GameCGAoptbtn=[]
    for i in range(6,8):
       GameCGAoptbtn=KeyboardButton(CGAOptlistEdt[i])
       GameCGAoptMarkup.add(GameCGAoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=GameCGAoptMarkup)
def GameCGAoptQuest(message):
    if(message.text==CGAOptlistEdt[6]):
        return True
    else:
        return False
def GameCGAoptSummary(message):
    if(message.text==CGAOptlistEdt[7]):
        return True
    else:
        return False
@bot.message_handler(func=GameCGAoptQuest)
def send_GameCGAoptQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1cNEYFlgafvetwl4N7k8Lc7P8nzeVf361?usp=drive_link")
@bot.message_handler(func=GameCGAoptSummary)
def send_GameCGAoptSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1GwU8Vdm8SkDgrfsfxy75npLrKMcn-IiN?usp=drive_link") 

#CGA Topics
@bot.message_handler(func=TopicCGAopt)
def send_TopicCGA(message):
    TopicCGAoptMarkup=ReplyKeyboardMarkup()
    TopicCGAoptbtn=[]
    for i in range(9,12):
       TopicCGAoptbtn=KeyboardButton(CGAOptlistEdt[i])
       TopicCGAoptMarkup.add(TopicCGAoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=TopicCGAoptMarkup)

def TopicCGAoptQuest(message):
    if(message.text==CGAOptlistEdt[9]):
        return True
    else:
        return False
def TopicCGAoptSummary(message):
    if(message.text==CGAOptlistEdt[10]):
        return True
    else:
        return False
def TopicCGAoptSlides(message):
    if(message.text==CGAOptlistEdt[11]):
        return True
    else:
        return False
@bot.message_handler(func=TopicCGAoptQuest)
def send_TopicCGAoptQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/18xBWzSRpbsY1T0nUrKQddETnmGU0lfbT?usp=drive_link")
@bot.message_handler(func=TopicCGAoptSummary)
def send_TopicCGAoptSummary(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1ijbxpOpWHXJ_cFme8Yxlc-httS5DLV8q?usp=drive_link") 
@bot.message_handler(func=TopicCGAoptSlides)
def send_TopicCGAoptSlides(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1RekBdmDmAXearmwPIDIRRiyC-WuLLSAq?usp=drive_link")

#CGA Game Programming
@bot.message_handler(func=GameProgCGAopt)
def send_GameProgCGAopt(message):
    GameProgCGAoptMarkup=ReplyKeyboardMarkup()
    GameProgCGAoptbtn=KeyboardButton(CGAOptlistEdt[12])
    GameProgCGAoptMarkup.add(GameProgCGAoptbtn)
    bot.send_message(message.chat.id,text="اختر ما تريد",reply_markup=GameProgCGAoptMarkup)
def GameProgCGAoptQuest(message):
    if(message.text==CGAOptlistEdt[12]):
        return True
    else:
        return False

@bot.message_handler(func=GameProgCGAoptQuest)
def send_CGAQuest(message):
    bot.send_message(message.chat.id,text="https://drive.google.com/drive/folders/1bPHKziK7Z5Lvt9GqY2S-1fMPAsC_ISeB?usp=drive_link")


#communication 
@bot.message_handler(func=communicate)
def send_communicate(message):
    bot.send_message(message.chat.id,text="https://t.me/+moFC8T6Xz49lNjA0")
#social
@bot.message_handler(func=social)
def send_social(message):
    bot.send_message(message.chat.id,text="Instagram: https://instagram.com/datastream_?igshid=YmMyMTA2M2Y=")
    bot.send_message(message.chat.id,text="FaceBook: https://m.facebook.com/Data-stream-100934195909145/")
    bot.send_message(message.chat.id,text="Youtube : https://www.youtube.com/channel/UCVP2PHNv2Wsj9OacmSCZjdA?si=ELPmzJkDCLju2KnD5oyZMQ")
#Suhaib's Channel
@bot.message_handler(func=teleChannel)
def send_teleChannel(message):
    bot.send_message(message.chat.id,text="https://t.me/+gnyyMS7vffM3Yjg0",protect_content=True)
"""
#count users
@bot.message_handler(commands=(['show']))
def show_user(message):     
     bot.send_message(message.chat.id, numOfUsers())
     print(get_user())

@bot.message_handler(commands=(['send']))
def send_message_to_phone(message):
    # Send message to your phone asking for the message to be sent to all users
    bot.send_message(message.chat.id, "Please reply with the message you want to send to all users.")
@bot.message_handler(func=lambda message: message.text.startswith('السلام'))
def process_response(message):
    # Check if the response is from your phone
    if message.chat.id == 792159482:
        # Extract the message provided by you
        message_to_send = message.text

        # Send the message to all users
        send_text(message_to_send)
def send_text(message):
    user_IDs=fetch_ID()
    for user_id in user_IDs['ID']:
        try:
            bot.send_message(user_id,message)
        except telebot.apihelper.ApiException as e:
            if e.result.status_code == 403 and "bot was blocked by the user" in e.result.text:
            # User has blocked the bot
                print(f"User {user_id} has blocked the bot.")
            # Handle the situation accordingly (e.g., disable certain functionalities for the user)
            else:
            # Handle other API errors
                print("API Error:", e)

"""
@bot.message_handler(commands=['stop'])
def stop_handler(message):
    bot.reply_to(message, "Goodbye! I'm no longer listening for messages.")
    bot.stop_polling()  # Stop polling for messages

from threading import Thread
from run_waitress import serve_flask_app  # Import the function from your Waitress script

# Your Flask application and Waitress setup
if __name__ == '__main__':
    Thread(target=serve_flask_app).start()
    
bot.infinity_polling()



    
