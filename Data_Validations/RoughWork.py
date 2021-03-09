






# Imputing missing values in a column
# ===================================
imputer=KNNImputer(n_neighbors=3, weights='uniform',missing_values=np.nan)
self.new_array=imputer.fit_transform(self.data) # impute the missing values
# convert the nd-array returned in the step above to a Dataframe
self.new_data=pd.DataFrame(data=self.new_array, columns=self.data.columns)