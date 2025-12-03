resource "aws_db_subnet_group" "orders_db_subnet" {
  name       = "orders-db-subnet"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_security_group" "db_sg" {
  name        = "orders-db-sg"
  description = "Security group for PostgreSQL"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    cidr_blocks     = ["10.0.0.0/16"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "orders_db" {
  identifier              = "orders-postgres"
  allocated_storage       = 20
  engine                  = "postgres"
  engine_version          = "15.5"
  instance_class          = "db.t3.micro"

  db_subnet_group_name   = aws_db_subnet_group.orders_db_subnet.name
  vpc_security_group_ids = [aws_security_group.db_sg.id]

  username = var.db_username
  password = var.db_password

  publicly_accessible = false
  skip_final_snapshot = true
}

