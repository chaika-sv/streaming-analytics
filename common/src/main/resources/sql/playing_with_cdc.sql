-- Создаём базу и простую таблицу заказов
CREATE DATABASE StreamingAnalyticsSource;
GO
USE StreamingAnalyticsSource;
GO

CREATE TABLE dbo.orders (
                            order_id INT IDENTITY PRIMARY KEY,
                            customer_name NVARCHAR(100),
                            product_name NVARCHAR(100),
                            amount DECIMAL(10, 2),
                            status NVARCHAR(20),
                            created_at DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

-- Включаем CDC на уровне базы данных — это создаёт служебные объекты
-- (схему cdc, системные таблицы для отслеживания изменений)
EXEC sys.sp_cdc_enable_db;
GO

-- Включаем CDC конкретно для таблицы orders. role_name = NULL означает
-- "без ограничений доступа к CDC-данным по ролям" — упрощение для пет-проекта.
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'orders',
    @role_name = NULL,
    @supports_net_changes = 0;
GO

