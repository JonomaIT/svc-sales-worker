regions = ["us-east-1", "sa-east-1"] # runs in both; only the ACTIVE region consumes (ADR-012)
hosts   = []                         # internal: no public name, nothing on the ALB

cpu          = 256
memory       = 512
min_capacity = 2
max_capacity = 4
