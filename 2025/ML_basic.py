# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 20:34:27 2025

@author: TeneroHuynh
"""

# Linear Regression
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
# Classification
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

# Generate simulated data
np.random.seed(42)
n = 100000
sqft = np.random.randint(500, 3000, n)         # Area in square feet
year_built = np.random.randint(1950, 2020, n)  # Year built
noise = np.random.normal(0, 5000, n)           # Random noise

# House price = 50*sqft + 300*(year - 1950) + noise
price = 50 * sqft + 300 * (year_built - 1950) + noise

# Put into DataFrame
df_reg = pd.DataFrame({
    'sqft': sqft,
    'year_built': year_built,
    'price': price
})

# Split X (input features) and y (target)
X_reg = df_reg[['sqft', 'year_built']]
y_reg = df_reg['price']

# Train-test split
X_train_r, X_test_r, y_train_r, y_test_r = train_test_split(X_reg, y_reg, test_size=0.2, random_state=42)

# Train Linear Regression model
model_reg = LinearRegression()
model_reg.fit(X_train_r, y_train_r)

# Predict and evaluate
y_pred_r = model_reg.predict(X_test_r)
mse = mean_squared_error(y_test_r, y_pred_r)

print("=== Regression Result ===")
print("Coefficients:", model_reg.coef_)
print("Intercept:", model_reg.intercept_)
print("MSE:", mse)


# Classification
# Generate simulated data
n = 100
tenure = np.random.randint(1, 60, n)            # Number of months as a customer
monthly_spend = np.random.randint(10, 100, n)   # Monthly spending

# Churn logic: low tenure + high spending -> churn
churn = ((tenure < 12) & (monthly_spend > 50)).astype(int)

# Put into DataFrame
df_clf = pd.DataFrame({
    'tenure': tenure,
    'monthly_spend': monthly_spend,
    'churn': churn
})

# Split X and y
X_clf = df_clf[['tenure', 'monthly_spend']]
y_clf = df_clf['churn']

# Train-test split
X_train_c, X_test_c, y_train_c, y_test_c = train_test_split(X_clf, y_clf, test_size=0.2, random_state=42)

# Train Logistic Regression
model_clf = LogisticRegression()
model_clf.fit(X_train_c, y_train_c)

# Predict and evaluate
y_pred_c = model_clf.predict(X_test_c)
acc = accuracy_score(y_test_c, y_pred_c)

print("\n=== Classification Result ===")
print("Coefficients:", model_clf.coef_)
print("Intercept:", model_clf.intercept_)
print("Accuracy:", acc)
