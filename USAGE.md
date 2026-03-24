# A股回测系统优化指南

## 问题与解决方案

### 原始问题
原始回测脚本 `backtest.py` 存在严重的数据重复下载问题：
- 每只股票 × 每个回测窗口都要单独调用API
- 相邻窗口有90%数据重叠，但被重复下载
- 1500只股票 × 1500个窗口 ≈ 225万次API调用
- 预计运行时间：62.5小时（纯API等待时间）

### 优化方案
引入智能缓存系统，核心改进：
1. **本地SQLite缓存**：存储已下载的K线数据
2. **智能数据获取器**：优先使用缓存，缺失部分增量下载
3. **批量预加载**：一次性获取股票的全局数据
4. **进度监控**：实时显示进度和性能统计

## 文件说明

### 新添加的文件
| 文件 | 用途 |
|------|------|
| `cache_schema.py` | 缓存数据库schema和基础操作 |
| `smart_fetcher.py` | 智能数据获取器（核心组件） |
| `backtest_optimized.py` | 优化版回测脚本（主入口） |
| `test_cache.py` | 缓存系统测试脚本 |
| `clean_emoji.py` | 清理Emoji字符的工具 |

### 优化后的性能对比
| 指标 | 原始方案 | 优化方案 | 提升倍数 |
|------|----------|----------|----------|
| API调用次数 | ~2,250,000 | ~1,500 | 1500× |
| 预计运行时间 | ~62.5小时 | ~25分钟 | 150× |
| 网络流量 | ~5GB | ~10MB | 500× |
| 错误恢复 | 无 | 缓存续传 | - |

## 快速开始

### 1. 环境设置
```bash
# 设置理杏仁API Token
export LIXINGER_TOKEN=your_token_here

# Windows (命令提示符)
set LIXINGER_TOKEN=your_token_here

# Windows (PowerShell)
$env:LIXINGER_TOKEN="your_token_here"
```

### 2. 运行优化版回测
```bash
# 完整回测（推荐预加载）
python backtest_optimized.py

# 跳过预加载（按需下载）
python backtest_optimized.py --no-preload

# 查看缓存统计
python backtest_optimized.py --stats

# 清除缓存数据
python backtest_optimized.py --clear-cache
```

### 3. 监控运行进度
优化版脚本提供实时进度监控：
- 每10个窗口显示一次进度
- 缓存命中率和API调用统计
- 预计剩余时间
- 内存使用情况

## 缓存系统详解

### 数据库结构
```
stock_cache.db
├── kline_cache           # K线数据表
│   ├── stock_code        # 股票代码
│   ├── date             # 交易日
│   ├── open/high/low/close/volume/amount/to_r
│   └── PRIMARY KEY (stock_code, date)
├── cache_metadata        # 元数据表
│   ├── stock_code       # 股票代码
│   ├── start_date/end_date  # 缓存日期范围
│   └── last_updated     # 最后更新时间
└── api_log              # API请求日志
```

### 智能获取逻辑
```
请求数据 → 检查缓存覆盖 → 完全命中 → 返回缓存数据
                ↓
          部分覆盖 → 下载缺失部分 → 合并数据 → 更新缓存
                ↓
          无缓存 → 下载全部数据 → 保存到缓存
```

### 缓存策略
1. **首次运行**：下载所有股票的全局数据到缓存
2. **后续运行**：直接使用缓存，无需API调用
3. **增量更新**：只下载缺失的日期范围
4. **强制刷新**：`force_refresh=True` 忽略缓存

## 高级用法

### 自定义配置
```python
# 修改 backtest_optimized.py 中的配置
CACHE_DB = "stock_cache.db"          # 缓存数据库路径
REQUEST_DELAY = 0.1                  # API请求延迟（秒）
BATCH_SIZE = 20                      # 批量预加载大小
GLOBAL_START_DATE = "2023-01-01"    # 回测开始日期
GLOBAL_END_DATE = "2025-01-01"      # 回测结束日期
```

### 程序化使用
```python
from smart_fetcher import SmartKlineFetcher
import pandas as pd

# 初始化获取器
fetcher = SmartKlineFetcher(
    token=os.getenv("LIXINGER_TOKEN"),
    cache_db="stock_cache.db",
    request_delay=0.1
)

# 获取单只股票数据
data = fetcher.get_kline_data("000001", "2024-01-01", "2024-12-31")

# 批量预加载
stock_codes = ["000001", "000002", "600519"]
fetcher.batch_preload(stock_codes, "2024-01-01", "2024-12-31")

# 获取统计信息
stats = fetcher.get_stats()
print(f"缓存命中率: {stats['cache_hit_rate']}%")
```

### 缓存管理
```python
from cache_schema import *

# 初始化数据库
init_cache_db("custom_cache.db")

# 查看缓存统计
print_cache_stats()

# 清除特定股票缓存
clear_cache("stock_cache.db", "000001")

# 获取缓存范围
range_info = get_cached_range("stock_cache.db", "000001")
```

## 故障排除

### 常见问题

#### 1. 编码错误（Windows）
**问题**: UnicodeEncodeError: 'gbk' codec can't encode character...
**解决**: 已通过 `clean_emoji.py` 移除所有Emoji字符

#### 2. API限流
**问题**: API返回错误或超时
**解决**: 调整 `request_delay` 参数（默认0.1秒）

#### 3. 缓存不更新
**问题**: 使用旧数据，不获取最新数据
**解决**: 使用 `force_refresh=True` 或清除缓存

#### 4. 内存不足
**问题**: 预加载太多股票导致内存不足
**解决**: 减小 `batch_size` 或跳过预加载

### 调试模式
```bash
# 设置详细日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 检查缓存内容
python -c "from cache_schema import print_cache_stats; print_cache_stats()"
```

## 性能优化建议

### 进一步优化
1. **并行下载**：使用多线程/多进程并发获取数据
2. **数据压缩**：对缓存数据进行压缩存储
3. **内存缓存**：添加Redis等内存缓存层
4. **增量回测**：只计算变化的窗口，复用已有结果

### 资源监控
- 使用 `fetcher.print_stats()` 查看API使用情况
- 监控 `stock_cache.db` 文件大小
- 记录每次运行的性能指标

## 从原始版本迁移

### 原始脚本
```bash
python backtest.py
```

### 优化脚本
```bash
# 第一次运行（建立缓存）
python backtest_optimized.py

# 后续运行（使用缓存）
python backtest_optimized.py --no-preload
```

### 主要区别
1. **数据获取**：从直接API调用改为智能缓存
2. **进度显示**：添加了详细的进度监控
3. **错误处理**：更完善的错误恢复机制
4. **续传功能**：支持中断后继续运行

## 注意事项

### 数据一致性
- 缓存数据基于前复权（`fc_rights`）
- 分红送股可能导致数据变化，建议定期清除缓存
- 使用 `force_refresh=True` 获取最新数据

### 存储要求
- 每只股票每年约250条K线记录
- 每条记录约100字节
- 1500只股票 × 5年 ≈ 1.8GB 原始数据
- SQLite压缩后约200-300MB

### API配额
- 理杏仁API有调用频率限制
- 默认0.1秒延迟符合大多数套餐限制
- 监控 `api_calls` 统计，避免超额

## 扩展开发

### 添加新因子
```python
# 在 backtest_optimized.py 的 run_backtest_by_index 函数中添加
def calculate_new_factor(df):
    """计算新因子示例"""
    # 使用缓存的数据进行计算
    return result
```

### 支持其他数据源
```python
class CustomFetcher(SmartKlineFetcher):
    """扩展支持其他数据源"""
    def _fetch_from_api(self, stock_code, start_date, end_date):
        # 实现自定义API调用
        pass
```

### 结果分析
```python
# 分析回测结果
import pandas as pd
import glob

# 加载所有结果文件
report_files = glob.glob("output/*_report.csv")
all_reports = pd.concat([pd.read_csv(f) for f in report_files])

# 进行统计分析
```

## 联系与支持

如有问题或建议，请参考代码注释或创建Issue。

## 版本历史
- v1.0: 初始版本，基础回测功能
- v2.0: 添加缓存系统，性能提升150倍
- v2.1: 修复Windows编码问题，添加使用文档