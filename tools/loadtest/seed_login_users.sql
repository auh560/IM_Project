USE llfc;

DROP PROCEDURE IF EXISTS llfc_seed_login_users;

DELIMITER //
CREATE PROCEDURE llfc_seed_login_users(IN user_count INT)
BEGIN
    DECLARE i INT DEFAULT 1;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    WHILE i <= user_count DO
        INSERT INTO `user` (`uid`, `name`, `email`, `pwd`, `nick`, `desc`, `sex`, `icon`)
        VALUES (
            100000 + i,
            CONCAT('loaduser', LPAD(i, 6, '0')),
            CONCAT('loaduser', LPAD(i, 6, '0'), '@example.com'),
            '745230',
            CONCAT('loaduser', LPAD(i, 6, '0')),
            '',
            0,
            ':/res/head_1.jpg'
        )
        ON DUPLICATE KEY UPDATE
            `name` = CONCAT('loaduser', LPAD(i, 6, '0')),
            `pwd` = '745230',
            `nick` = CONCAT('loaduser', LPAD(i, 6, '0')),
            `desc` = '',
            `sex` = 0,
            `icon` = ':/res/head_1.jpg';

        SET i = i + 1;
    END WHILE;

    IF EXISTS (SELECT 1 FROM `user_id`) THEN
        UPDATE `user_id`
        SET `id` = (SELECT MAX(`uid`) FROM `user`);
    ELSE
        INSERT INTO `user_id` (`id`)
        SELECT MAX(`uid`) FROM `user`;
    END IF;

    COMMIT;
END//
DELIMITER ;

CALL llfc_seed_login_users(5000);
DROP PROCEDURE llfc_seed_login_users;

SELECT COUNT(*) AS load_test_users
FROM `user`
WHERE `email` LIKE 'loaduser%@example.com';
