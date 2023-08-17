def isLocked(input_file):

    '''Checks to see if a file/filepath is locked.
    1. Attemps to open the file. This will determine if the file has a lock or not.
    If opening then return status would be unlock.
    2. If this fails the file is open by some other process or locked by some other process then in that situation return would be locked '''

    try:
        open(input_file)
        df=pd.read_csv(input_file)
        return 'Unlock'
    except:
        return 'Locked'

def lockwait(TABLE):
    today=date.today()
    SYSDATE=today.strftime('%d-%b-%Y')
    status=isLocked(TABLE)
    CURRENT=datetime.now().strftime('%H:%M:%S')

    #Checking if a file is previously locked or not
    if (status=="Unlock"):
        display(HTML('<FONT color=#4169E1><H3>LOCK EN ATTENTE SUR '+(TABLE) +' ...</H3><S>')) 
        file = open (TABLE, 'r+')
        portalocker.lock(file, portalocker.LOCK_EX)
        display(HTML('<FONT color=#4169E1><H3>CURRENT +(CURRENT)+ ...</H3><S>'))
        display(HTML('</S><FONT color=#4169E1><H3>LOCK OBTENU SUR+(TABLE) +==>! + (SYSDATE)+'/+(CURRENT) +'</H3></FONT>'))
    else:
        display(HTML('<FONT color=#4169E1><H4>'+'SYSDATE=+ (SYSDATE)+...</H4><S>'))
        display(HTML('</S><FONT color=#4169E1><H4>Table ' + (TABLE) + 'non disponible A'+ (SYSDATE)+'/'+ (CURRENT) +'==> Attendons encore un peu ...</H4></FONT><S>'))
        time.sleep(5)
        CURRENT_1=datetime.now().strftime('%H-%M-%S')
        display(HTML('<FONT color=#4169E1<H4>'+(CURRENT_1)+'..</H4><S>'))
        _ERROR_=0