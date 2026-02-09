resource "aws_db_subnet_group" "db_group" {
  name       = "${var.project}-db-subnet"
  subnet_ids = var.subnets
}

resource "aws_db_instance" "db" {
  allocated_storage    = 20
  engine               = "postgres"
  engine_version       = "15"
  instance_class       = "db.t3.micro"
  username             = var.db_username
  password             = var.db_password
  db_subnet_group_name = aws_db_subnet_group.db_group.name
}

