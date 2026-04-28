class resuable_bundle:

    def dropColumns(self, df, column):
        # if list of cols, then * unpack the list into strings
        df = df.drop(*column) 
        return df