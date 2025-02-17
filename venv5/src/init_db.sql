CREATE TABLE IF NOT EXISTS usuarios (
    id INT AUTO_INCREMENT PRIMARY KEY,
    mail_contact VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS propiedades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100),
    city VARCHAR(100),
    colony VARCHAR(100),
    street VARCHAR(255),
    external_num VARCHAR(50),
    code VARCHAR(50) UNIQUE,
    type VARCHAR(50),
    purpose VARCHAR(50),
    price DECIMAL(10,2),
    mail_contact VARCHAR(255),
    phone_contact VARCHAR(50),
    user_id INT,
    FOREIGN KEY (user_id) REFERENCES usuarios(id)
);
