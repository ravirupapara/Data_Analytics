def lockclear(df):
    df_html=df.to_html()
    # Generate a random identifier for the table and style we are going to create.
    random_id='id%d' % np.random.choice(np.arange(1000000))

    #For interesting that style in our table
    df_html=re.sub(r'<table',r'<table id=%s' % random_id,df_html)

    # Create a style tag
    style="""<style>
            table #{random_id}{{color:#4169E1}}
            </style>
            <H3>DATASET TABLE AGAIN AVAILABLE FOR OTHER PROCESS:</H3>""".format(random_id=random_id)
    table_n=HTML(style + df_html)

    return table_n