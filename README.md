# Project: Router Automation via Kafka (GNS3 Lab)

ระบบบริหารจัดการและส่งคำสั่งไปยังเราเตอร์ Cisco ใน GNS3 Lab ผ่านสถาปัตยกรรม Asynchronous Event-Driven ด้วย Apache Kafka, FastAPI, Paramiko SSH และ Real-time WebSocket Monitoring

---

## 1. Architecture Overview

```
Frontend (HTML/JS + Tailwind)
     │  ▲
     │  │  WebSocket (Real-time updates)
HTTP │  │
POST │  │
     ▼  │
Backend (FastAPI + PostgreSQL) ──produce──▶ [Kafka: router-commands] (3 Partitions, Key: router_id)
     ▲                                                │
     │ consume                                        │ consume (Group: router-workers)
     │                                                ▼
[Kafka: router-results] ◀──────produce────── Worker (Python + Paramiko SSH)
(Key: router_id)                                      │
                                                      ├──▶ SSH: Router 1 / Router 2 (GNS3)
                                                      └──▶ [Kafka: router-dlq] (เมื่อ Retry ครบ 3 ครั้งแล้วยังล้มเหลว)
```

### ทำไมต้องใช้ Kafka คั่นกลาง?
- **Non-blocking & Asynchronous**: การ SSH เข้าอุปกรณ์เครือข่ายอาจใช้เวลา 2-10 วินาที Backend จึงไม่ควรรอ ให้ตอบกลับผู้ใช้ทันทีด้วย `job_id` (< 200 ms)
- **FIFO Per Device**: แต่ละ Router ถูก map เข้า Partition ตาม `router_id` ทำให้คำสั่งของ Router ตัวเดียวกันไม่ถูกรันพร้อมกันและไม่สลับลำดับ
- **Fault-Tolerance & DLQ**: หากเราเตอร์ดับหรือเน็ตเวิร์กขัดข้อง Worker จะ Retry แบบ Exponential Backoff (3 ครั้ง) หากยังไม่สำเร็จจะส่งเข้า Dead Letter Queue (`router-dlq`) เพื่อนำไปตรวจสอบต่อ

---

## 2. โครงสร้างโฟลเดอร์

```
Mahama LAB/
├── docker-compose.yml       # Kafka (KRaft), Kafka UI (8080), PostgreSQL (5432)
├── backend/
│   ├── main.py              # FastAPI REST API & WebSocket endpoints
│   ├── models.py            # SQLAlchemy Model & Pydantic schemas
│   ├── database.py          # PostgreSQL connection pool & session
│   ├── actions.py           # Whitelist actions & interface regex validation
│   ├── kafka_producer.py    # Key-based Kafka Producer (router-commands)
│   ├── kafka_consumer.py    # Consumer for router-results & WebSocket broadcast
│   ├── websocket_manager.py # WebSocket Client Manager
│   ├── requirements.txt
│   └── .env
├── frontend/
│   ├── index.html           # Modern Web Dashboard (Tailwind CSS CDN)
│   └── app.js               # Form dispatch, live table, output inspection drawer
├── worker/
│   ├── main.py              # Consumer loop, retry backoff, manual commit, DLQ
│   ├── ssh_client.py        # Paramiko SSH client, prompt detection, terminal length 0
│   ├── actions.py           # Action to CLI commands translator
│   ├── config.py            # routers.yaml loader with env credentials
│   ├── routers.yaml         # Router IP/port inventory
│   ├── requirements.txt
│   └── .env
└── README.md
```

---

## 3. การเตรียม Cisco Router ใน GNS3

ใน GNS3 เชื่อมต่อ Router (เช่น c7200 หรือ c3725) เข้ากับ **Cloud / NAT Node** หรือ Management Subnet ที่เครื่องโฮสต์เข้าถึงได้ จากนั้นคอนฟิก SSH บนเราเตอร์:

```cisco
configure terminal
hostname R1
ip domain-name lab.local
crypto key generate rsa modulus 2048
ip ssh version 2
ip ssh time-out 60
ip ssh authentication-retries 3

username admin privilege 15 secret cisco123

line vty 0 4
 login local
 transport input ssh
 exit

interface FastEthernet0/0
 ip address 192.168.122.11 255.255.255.0
 no shutdown
 exit
end
write memory
```

*(ทำแบบเดียวกันบน R2 โดยเปลี่ยน IP เป็น `192.168.122.12`)*

---

## 4. ขั้นตอนการติดตั้งและรันระบบ

### 4.1 เริ่มต้น Kafka & PostgreSQL ด้วย Docker Compose

```powershell
docker compose up -d
```

ตรวจสอบสถานะ:
- **Kafka Broker**: `localhost:9092`
- **Kafka UI**: เข้าเบราว์เซอร์ที่ [http://localhost:8080](http://localhost:8080) เพื่อดู Topic ทั้ง 3:
  - `router-commands` (3 Partitions)
  - `router-results` (3 Partitions)
  - `router-dlq` (1 Partition)
- **PostgreSQL**: `localhost:5432` (Database: `router_lab`)

---

### 4.2 ติดตั้ง Dependencies

#### Backend:
```powershell
cd "D:\Mahama LAB\backend"
python -m pip install -r requirements.txt
```

#### Worker:
```powershell
cd "D:\Mahama LAB\worker"
python -m pip install -r requirements.txt
```

---

### 4.3 เริ่มต้น Backend Service

เปิด PowerShell หน้าต่างที่ 1:
```powershell
cd "D:\Mahama LAB\backend"
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
- Web UI & API พร้อมใช้งานที่: [http://localhost:8000](http://localhost:8000)
- API Docs (Swagger): [http://localhost:8000/docs](http://localhost:8000/docs)

---

### 4.4 ปรับแต่งการเชื่อมต่อ Router และรัน Worker

แก้ไขไฟล์ [worker/routers.yaml](file:///D:/Mahama%20LAB/worker/routers.yaml) ให้ IP ตรงกับเราเตอร์ใน GNS3 ของคุณ:
```yaml
routers:
  router1:
    host: 192.168.122.11
    port: 22
    username_env: ROUTER_USER
    password_env: ROUTER_PASS
  router2:
    host: 192.168.122.12
    port: 22
    username_env: ROUTER_USER
    password_env: ROUTER_PASS
```

และกำหนดรหัสผ่านใน [worker/.env](file:///D:/Mahama%20LAB/worker/.env):
```env
ROUTER_USER=admin
ROUTER_PASS=cisco123
```

เปิด PowerShell หน้าต่างที่ 2 เพื่อรัน Worker:
```powershell
cd "D:\Mahama LAB\worker"
python main.py
```

---

## 5. การทดสอบตามเกณฑ์ยอมรับ (Acceptance Criteria)

| ข้อกำหนด | วิธีทดสอบ | ผลลัพธ์ที่คาดหวัง |
|---|---|---|
| **1. Kafka Topics** | เปิด [http://localhost:8080](http://localhost:8080) | พบ topics `router-commands`, `router-results`, `router-dlq` |
| **2. Asynchronous Response (<200ms)** | ส่ง `POST /api/jobs` หรือกดสั่งงานจากหน้าเว็บ | ได้รับ `job_id` ทันทีและสถานะ `pending` โดยไม่ต้องรอ SSH |
| **3. SSH Execution & Real-time Update** | เลือก `router1` และ Action `show_interfaces` | Worker SSH สำเร็จ ส่งผลลัพธ์ผ่าน Kafka สถานะเปลี่ยนเป็น `success` และมี Output แสดงในหน้าเว็บโดยไม่ต้อง Refresh |
| **4. Defense-in-Depth Validation** | สั่ง Action ที่ไม่มีใน Whitelist หรือ Interface ผิดฟอร์แมต | Backend ตอบ HTTP `400 Bad Request` ทันที และไม่มี Message ถูกส่งเข้า Kafka |
| **5. Retry & Dead Letter Queue (DLQ)** | ปิด Router ใน GNS3 แล้วส่งงานไป | Worker พยายาม Retry 3 ครั้งแบบ Exponential Backoff เมื่อครบจะส่งงานเข้า `router-dlq` และแจ้งสถานะ `failed` |
| **6. At-least-once & Offset Commit** | ปิด Worker ระหว่างรัน แล้วเปิดใหม่ | Offset ที่ยังไม่ commit จะถูก Worker นำกลับมาประมวลผลใหม่โดยงานไม่สูญหาย |

---

## 6. Action Whitelist ที่รองรับ

| Action | Parameter | คำสั่ง IOS ที่ Worker สร้าง |
|---|---|---|
| `show_interfaces` | - | `terminal length 0` <br> `show ip interface brief` |
| `show_running_config` | - | `terminal length 0` <br> `show running-config` |
| `show_routes` | - | `terminal length 0` <br> `show ip route` |
| `interface_shutdown` | `interface` (เช่น `FastEthernet0/0`) | `configure terminal` <br> `interface <x>` <br> `shutdown` <br> `end` |
| `interface_no_shutdown` | `interface` (เช่น `FastEthernet0/0`) | `configure terminal` <br> `interface <x>` <br> `no shutdown` <br> `end` |
