# CatalogSync

用于网页/API分页采集、来源字段标准化、版本归并和结构化落库的数据采集管道。提供Python服务接口、HTTP操作接口及命令行操作入口。商品是演示数据，不包含购物、订单、库存管理或Excel导入业务。

## 运行

Python 3.11或以上，仅使用标准库。在源码根目录直接运行，不要求安装Python包。用户当前运行位置为远程ECS Linux Docker；下面命令在该环境执行。

```bash
python3 -m unittest discover -s tests -v
python3 -m catalogsync --home /tmp/catalogsync-demo demo --http
python3 -m catalogsync --home /tmp/catalogsync-state serve --host 0.0.0.0 --port 8761
```

`demo --http`临时启动本地供应商HTTP站点，真实采集两页数据、隔离错误行并生成可销售数据集。退出时关闭演示站点，保留指定home中的采集结果。源码中的examples只用于演示；注册实际来源后不依赖examples。命令行是管道操作入口，主要能力由Python领域服务提供。

## 数据链路

分页来源 → 原始页归档 → JSON/HTML/JSON-LD提取 → 冻结的来源映射与质量规则 → 版本归并 → 事务提交商品及进度 → 不可变数据集发布 → 独立SQLite消费端物化。

增量入口包括直接提交记录、带事件ID的持久化收件箱及追加式NDJSON事件文件。收件箱保留事件处理结果，事件身份冲突被拒绝；文件入口等待完整换行并保存字节偏移，文件替换或截断不会静默重置进度。

## 来源协议

文件来源目录包含manifest.json，其pages对象将cursor映射到相对页文件。初始cursor为空字符串。HTTP来源使用`GET <base>/catalog?cursor=...`，按来源限速、限制并发并处理临时错误和Retry-After。

JSON页包含snapshot_id、boundary、cursor、next_cursor及records。相同快照所有页的snapshot_id和boundary必须一致；next_cursor为null才表示结束。不接受自环、返回旧cursor或中途改变快照身份。

同一来源的revision和boundary使用同一可比较的源端版本序。boundary是完整快照的源端截点，记录revision不得超过它；不是抓取时间或本地收件序号。来源保证该snapshot_id的分页集合稳定。不同来源的版本不比较。

HTML来源在meta中提供catalog:snapshot、catalog:boundary及可选catalog:cursor；下一页使用`link rel="next" data-cursor="..."`。商品article通过data-sku、data-revision及data-field标注。JSON-LD支持Product、@graph和ItemList，用catalogRevision表达来源版本，offers提供价格及可选inventoryLevel。

## 基线行为

- 商品身份为(source_id, sku)，较新版本替代较旧版本；同版本相同内容重复读取不改结果，同版本不同内容进入拒绝记录。
- 旧版本可以作为历史观测保存，但不能覆盖当前版本。不同来源的同名SKU相互独立。
- 逐行数据问题被隔离，页级协议问题使运行失败。商品、版本、拒绝行、页计数和下一cursor在同一事务内提交。
- 失败运行可resume，从已提交cursor继续；已完成运行再次执行不重新采集。来源映射和质量策略在运行创建时冻结，恢复不受后续配置修改影响。
- 当前只合并来源返回的记录，不推断“本次未出现”意味着删除。部分页、含拒绝行或重新抓取，都不会自动删除已有商品。
- 发布读取一致的数据库快照，产物有校验和，旧发布文件不改。消费端按发布的来源范围事务替换；拒绝未曾应用过的过期发布，重复应用相同发布不改结果。

## 来源映射或清洗规则修正后的重处理

来源映射或质量规则修正后，归档里的原始页需要按新规则重新解析，但源端版本并未改变。重处理把“源端版本”（记录的revision及快照boundary）和“清洗口径”（来源映射版本）分开：同一源端revision可按每个口径各记录一次解释，因此按新口径重放旧归档不会被当作同版本冲突。

- `reprocess <source>` 选择该来源最近一次已完成运行的归档页，用当前（或 `--profile-version` 指定、`--run` 指定基准运行）的口径逐页重放，结果先写入独立暂存表，不触碰当前商品。
- 只有整批归档页全部成功暂存后，才在单个事务内切换为当前有效结果；任一页失败则当前结果、既有运行、拒绝行和已发布文件全部保持原样。
- 失败的重处理可 `reprocess-resume <job>` 从已提交的页序号继续，不重复已暂存页。
- 重处理进行期间经正常入口到达的更高源端revision按新口径写入，并属于切换后的有效结果；旧归档不会把已上线的更高版本回退。切换后发布与下游物化都读取切换后的有效结果。
- 切换不会借更换规则放过来源同版本的真实冲突：同一源端revision对应的原始记录内容不一致时，该记录仍进入拒绝记录，当前商品保留原内容。

```bash
python3 -m catalogsync --home /tmp/state profile-set supplier new-profile.json
python3 -m catalogsync --home /tmp/state reprocess supplier
python3 -m catalogsync --home /tmp/state reprocess-jobs --source supplier
python3 -m catalogsync --home /tmp/state reprocess-balance <job>
python3 -m catalogsync --home /tmp/state reprocess-resume <job>
```

## 常用操作

```bash
python3 -m catalogsync --home /tmp/state source-add supplier examples/supplier
python3 -m catalogsync --home /tmp/state pull supplier
python3 -m catalogsync --home /tmp/state products --source supplier
python3 -m catalogsync --home /tmp/state publish --source supplier
python3 -m catalogsync --home /tmp/state check --archives
python3 -m catalogsync --home /tmp/state collect --workers 2 --publish
```

使用`python3 -m catalogsync --help`查看映射、来源事件、追加文件、拒绝行回放及消费端操作。HTTP接口与CLI调用同一领域服务。服务无身份认证，默认仅监听127.0.0.1，用于可信隔离网络，不直接暴露公网。

## 持久化与现场

home中包含catalog.sqlite、archive和datasets；check核对关系及可选原始页校验和。程序不会在正常结束后删除状态。

仅确认失败且原执行者已结束的运行可续采；running进程不能由另一执行者接管。线程同步与SQLite事务保护正常并行入口；强行终止进程后的状态需操作者先确认现场，不把日志静止当作执行结束。

作者题面、答案、私有验收材料及服务器凭据不属于此源码仓库。
