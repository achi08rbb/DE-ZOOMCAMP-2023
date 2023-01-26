# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=dtc-de-course-375015"

# Create new infra
terraform apply -var="project=dtc-de-course-375015"