FROM quay.io/astronomer/astro-runtime:12.0.0

# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
