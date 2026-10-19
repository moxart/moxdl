# mox

[English](README.md)

دانلود منیجر ترمینال. هر فایل را بین چند اتصال تقسیم می‌کند، اگر وسط کار متوقفش کنی از همان‌جا ادامه می‌دهد، و با قطع و وصل شدن اینترنت کنار می‌آید. کل برنامه یک فایل پایتون است و به هیچ کتابخانهٔ دیگری نیاز ندارد.

![mox در حال دانلود سه فایل هم‌زمان؛ نوار پیشرفت هر فایل نشان می‌دهد کدام بخش‌ها دانلود شده](docs/demo/demo.svg)

## نصب

```bash
curl -fsSL https://raw.githubusercontent.com/moxart/moxdl/main/mox.py -o ~/.local/bin/mox && chmod +x ~/.local/bin/mox
```

پایتون ۳.۸ یا جدیدتر لازم است. یا با pipx:

```bash
pipx install moxdl
```

روی اندروید با [Termux](https://termux.dev): اول `pkg install python`، بعد همان دستور `curl` بالا، با `$PREFIX/bin/mox` به‌جای `~/.local/bin/mox`.

## چرا؟

بعضی سرورها سرعت هر اتصال را جدا محدود می‌کنند، پس اتصال بیشتر یعنی سرعت بیشتر. IDM هم همین کار را می‌کند.

دانلود یک فایل ۱۰۰ مگابایتی از سرور محلی‌ای که هر اتصال را به ۴ مگابایت بر ثانیه محدود کرده:

| دستور | زمان | سرعت |
| --- | ---: | ---: |
| `wget` | ۲۴.۵ ثانیه | 3.9 MB/s |
| `curl` | ۲۴.۴ ثانیه | 3.9 MB/s |
| `mox -c 4` | ۶.۳ ثانیه | 15.0 MB/s |
| `mox` (۱۶ اتصال) | ۱.۸ ثانیه | 51.7 MB/s |
| `mox -c 32` | ۱.۱ ثانیه | 88.2 MB/s |

همهٔ سرورها این‌طور نیستند. برای این‌که بفهمی یک سرور چند اتصال قبول می‌کند:

```bash
mox bench https://example.com/big.iso
```

چیزی ذخیره نمی‌کند و می‌گوید با `-c` چند اتصال بدهی.

## کاربردهایی که احتمالاً به کارت می‌آید

### دانلود شبانه

اگر بستهٔ اینترنت شبانه داری:

```bash
mox --at 02:00 --until 07:00 -i links.txt
```

ساعت ۲ شروع می‌کند، ساعت ۷ مکث می‌کند، و شب بعد ادامه می‌دهد تا همه‌چیز تمام شود. ترمینال باید باز بماند و کامپیوتر نخوابد.

### خبر دادن در تلگرام

صبح که بیدار شدی، پیام «دانلودها تمام شد» در تلگرامت است:

```bash
export MOX_TELEGRAM_TOKEN=123456:ABC...   # از @BotFather
export MOX_TELEGRAM_CHAT=987654321        # شناسهٔ چت خودت، مثلاً از @userinfobot
mox --notify --at 02:00 --until 07:00 -i links.txt
```

اول با [@BotFather](https://t.me/BotFather) یک ربات بساز و یک پیام به آن بفرست. بدون تلگرام، `--notify` فقط نوتیفیکیشن دسکتاپ نشان می‌دهد.

### پروکسی

```bash
mox --proxy socks5://127.0.0.1:1080 https://example.com/file.iso
mox --proxy socks5h://user:pass@host:1080 https://example.com/file.iso
mox --proxy http://127.0.0.1:8080 https://example.com/file.iso
```

### لینکی که منقضی شد

اگر لینک وسط دانلود منقضی شود، mox دستور ادامه را چاپ می‌کند. لینک تازه بگیر و با همان اسم فایل بده:

```bash
mox -d ~/Downloads -o big-file.iso 'https://host.example/لینک-جدید'
```

از همان بایتی که مانده بود ادامه می‌دهد.

### لینک را کپی کن، فایل دانلود می‌شود

```bash
mox --watch -d ~/Downloads
```

هر لینک فایلی که کپی کنی دانلود می‌شود، مثل IDM ولی بدون افزونهٔ مرورگر. روی لینوکس به `wl-clipboard` یا `xclip` یا `xsel` نیاز دارد.

### ادامهٔ همه‌چیز با یک دستور

```bash
cd ~/Downloads && mox resume
```

همهٔ دانلودهای نیمه‌کارهٔ این پوشه را ادامه می‌دهد. لازم نیست دستور اول یادت باشد.

## استفادهٔ معمولی

```bash
mox https://example.com/file.iso                 # یک فایل
mox -d ~/Downloads -j 2 URL1 URL2               # چند فایل، دوتا دوتا
mox -i links.txt                                 # از روی فهرست
mox -l 2M https://example.com/big.mkv            # با سقف سرعت ۲ مگابایت
mox -H "Cookie: session=..." URL                 # فایلی که لاگین می‌خواهد
mox --sha256 9f86d0... URL                       # بررسی سلامت فایل
```

با Ctrl+C مکث می‌کند. همان دستور را دوباره بزن تا ادامه بدهد، حتی بعد از ریستارت.

فهرست کامل گزینه‌ها و محدودیت‌ها در [README انگلیسی](README.md) است.

## از داخل پایتون

```python
import mox
path = mox.download("https://example.com/big.iso", "downloads")
```

## مجوز

MIT
