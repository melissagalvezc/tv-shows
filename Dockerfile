FROM quay.io/astronomer/astro-runtime:10.4.0

# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
