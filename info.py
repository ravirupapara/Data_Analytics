#importing the python programs
exec(open('lockclear.py')).read()
exec(open('lockwait.py')).read()

def info(df):
    global LstMV
    lockwait(df)
    
    LstMV=list(df['MACROV'])
    
    MACROV=list(df['VALUER'].str.strip())
    
    info=dict(zip(LstMV,MACROV))
    
    df=lockclear(df)

    return info