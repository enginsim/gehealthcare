-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
-- -----------------------------------------------------
-- Schema gehealthcare
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema gehealthcare
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `gehealthcare` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci ;
USE `gehealthcare` ;

-- -----------------------------------------------------
-- Table `gehealthcare`.`allianz_data`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `gehealthcare`.`allianz_data` ;

CREATE TABLE IF NOT EXISTS `gehealthcare`.`allianz_data` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `country` VARCHAR(255) NULL DEFAULT NULL,
  `medium_term_rating` VARCHAR(50) NULL DEFAULT NULL,
  `short_term_rating` VARCHAR(50) NULL DEFAULT NULL,
  `risk_level` VARCHAR(50) NULL DEFAULT NULL,
  `year_quarter` VARCHAR(10) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `uq_country_quarter` (`country` ASC, `year_quarter` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 1447
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `gehealthcare`.`countryeconomy_data`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `gehealthcare`.`countryeconomy_data` ;

CREATE TABLE IF NOT EXISTS `gehealthcare`.`countryeconomy_data` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `country` VARCHAR(255) NOT NULL,
  `rating_agency` VARCHAR(255) NOT NULL,
  `rating` VARCHAR(255) NOT NULL,
  `rating_date` DATE NOT NULL,
  `term_type` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `uq_country_ratingdate` (`country` ASC, `rating_date` DESC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 3663
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `gehealthcare`.`worldbank_data`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `gehealthcare`.`worldbank_data` ;

CREATE TABLE IF NOT EXISTS `gehealthcare`.`worldbank_data` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `codeindyr` VARCHAR(50) NULL DEFAULT NULL,
  `code` VARCHAR(20) NULL DEFAULT NULL,
  `countryname` VARCHAR(255) NULL DEFAULT NULL,
  `year` INT NULL DEFAULT NULL,
  `indicator` VARCHAR(255) NULL DEFAULT NULL,
  `estimate` FLOAT NULL DEFAULT NULL,
  `stddev` FLOAT NULL DEFAULT NULL,
  `nsource` INT NULL DEFAULT NULL,
  `pctrank` FLOAT NULL DEFAULT NULL,
  `pctranklower` FLOAT NULL DEFAULT NULL,
  `pctrankupper` FLOAT NULL DEFAULT NULL,
  `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `codeindyr` (`codeindyr` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 154871
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
