# v1 存量采集状态

这是由原始 v1 程序真实生成的离线存量，不是迁移结果。state/catalog.sqlite、原始响应、两个已发布文件和 consumer.sqlite 属于同一现场。CNY-1=1.25，JPY-1=100，JPY-2=0.50；旧格式均以百分之一计数。JPY-2 用于验证新格式不能精确表达的存量。

请在独立副本上迁移。来源登记中的 /tmp/catalogsync-legacy-v1/source 是生成现场的历史地址，不是当前可抓取站点；本题不依赖访问该地址。用 Application(复制后的 state 目录) 读取离线源状态，消费库是同级 consumer.sqlite。发布文件路径相对 state/datasets，可在迁移前后直接核对字节和校验和。样本不含网络凭据。
