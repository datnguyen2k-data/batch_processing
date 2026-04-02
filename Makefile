.PHONY: add-lib build run-job bash

# Lệnh 1: Dùng uv để add thư viện vào dự án
# Ví dụ: make add-lib LIB="pandas numpy"
add-lib:
	uv add $(LIB)
	@echo "Đã cập nhật uv.lock. Đang tiến hành build lại Image..."
	$(MAKE) build

# Lệnh 2: Build lại Docker image cực nhanh bằng uv cache
build:
	docker compose build

# Lệnh 3: Chạy nhanh 1 file Spark Job trên Dev
# Ví dụ: make run-job JOB=examples/test.py
run-job:
	docker exec -it spark-master bash -c "cd /opt/spark/work-dir && spark-submit $(JOB)"

# Lệnh 4: Chui vào trong Container của Master
bash:
	docker exec -it spark-master bash
