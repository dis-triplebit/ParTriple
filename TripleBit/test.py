import numpy as np
y = [0.02,0.5285,0.7751,0.0609]
x = [[1046,13264,30178,7960],[1,2,2,1]]
X = np.column_stack(x+[[1]*len(x[0])])
beta_hat = np.linalg.lstsq(X,y)[0]
print beta_hat
print np.dot(X,beta_hat)

print beta_hat[0]*210716 + beta_hat[1]*2 + beta_hat[2]
print beta_hat[0]*58395 + beta_hat[1]*1 + beta_hat[2]
