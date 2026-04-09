import urllib.request
import urllib.error
import json
from src.shared.logger import get_logger

logger = get_logger("WebhookLogger")

class CentralizedWebhookLogger:
    """
    Communicates execution state back to the TypeScript Control Plane.
    This replaces tight-coupled SQLAlchemy Database audits inside Spark containers.
    """
    def __init__(self, run_id: str, control_plane_url: str):
        self.run_id = run_id
        self.base_url = control_plane_url.rstrip("/")
        
    def report_status(self, status: str, details: dict = None):
        """
        Status should be RUNNING, SUCCESS, or FAILED.
        """
        payload = {"status": status}
        if details:
            payload.update(details)
            
        url = f"{self.base_url}/api/audit/{self.run_id}/status"
        
        try:
            logger.info(f"Sending webhook to {url}: {status}")
            req = urllib.request.Request(
                url, 
                data=json.dumps(payload).encode('utf-8'),
                headers={'Content-Type': 'application/json'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                pass
        except urllib.error.URLError as e:
            # Control plane unavailability shouldn't fail the data payload
            logger.error(f"Failed to deliver audit webhook for {self.run_id}: {str(e)}")
