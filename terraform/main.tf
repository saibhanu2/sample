provider "aws" {
  region = var.aws_region
}

resource "aws_vpc" "vpc1" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  tags = {
    Name = var.vpc_Name
  }

}

resource "aws_subnet" "public-subnet1" {
  vpc_id     = aws_vpc.vpc1.id
  cidr_block = var.public_subnet1_cidr
  tags = {
    Name = var.public_subnet1_Name
  }

}

resource "aws_subnet" "public-subnet2" {
  vpc_id     = aws_vpc.vpc1.id
  cidr_block = var.public_subnet2_cidr
  tags = {
    Name = var.public_subnet2_Name
  }

}

resource "aws_subnet" "private-subnet1" {
  vpc_id     = aws_vpc.vpc1.id
  cidr_block = var.private_subnet1_cidr
  tags = {
    Name = var.private_subnet1_Name
  }

}

resource "aws_subnet" "private-subnet2" {
  vpc_id     = aws_vpc.vpc1.id
  cidr_block = var.private_subnet2_cidr
  tags = {
    Name = var.private_subnet2_Name
  }

}

resource "aws_internet_gateway" "igw-1" {
  vpc_id = aws_vpc.vpc1.id
  tags = {
    Name = "igw-1"
  }

}

resource "aws_route_table" "rt-2" {
  vpc_id = aws_vpc.vpc1.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw-1.id
  }
  tags = {
    Name = "igw-1"
  }
}

resource "aws_instance" "instance1" {
  ami                         = var.ami
  subnet_id                   = "subnet-08d8788d205fe16dd"
  associate_public_ip_address = true
  vpc_security_group_ids      = ["sg-03624f396d534e422"]
  instance_type               = var.instance_type
  key_name                    = var.key_name
  tags = {
    Name = var.instance_Name
  }

}