#####################################################################################################
# The ECR repositories are created outside of terraform please see:                                 #
# https://skillsforcare.atlassian.net/wiki/spaces/DE/pages/1493762050/Lambda+deployment+via+Docker  #
# for more information on how to create one                                                         #
#####################################################################################################

# Get the latest image digests for this branch
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "map-public-ip-on-launch"
    values = ["true"]
  }
}
