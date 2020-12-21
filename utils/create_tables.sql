CREATE TABLE IF NOT EXISTS `chargeable` (
	`id` INT NOT NULL AUTO_INCREMENT,
	`partner_id` INT,
	`product` VARCHAR(200),
	`partner_purchased_plan_id` VARCHAR(200),
	`plan` VARCHAR(200),
	`usage` INT,
	PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `domains` (
	`id` INT NOT NULL AUTO_INCREMENT,
	`partner_purchased_plan_id` VARCHAR(200),
	`domains` VARCHAR(800) UNIQUE,
	 PRIMARY KEY (`id`)
);
