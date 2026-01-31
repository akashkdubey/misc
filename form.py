"""
Output formatters for Copy Block Generator.
Exports results to CSV, JSON, and Excel with flattened structure for easy validation.
"""

import csv
import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

# Try to import openpyxl for Excel support
try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False


class OutputFormatter:
    """Format and save workflow outputs to various file formats."""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save_all(self, result: Dict[str, Any], base_name: str = None) -> Dict[str, Path]:
        """Save result to all formats: JSON, CSV, and Excel."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        query = result.get('query', 'output')
        safe_query = "".join(c if c.isalnum() or c in ' -_' else '_' for c in query)[:30]
        base_name = base_name or f"{safe_query}_{timestamp}"
        
        paths = {}
        
        # JSON - full output
        paths['json'] = self.save_json(result, base_name)
        
        # CSV - flattened
        paths['csv'] = self.save_csv(result, base_name)
        
        # Excel - validation friendly
        if EXCEL_AVAILABLE:
            paths['excel'] = self.save_excel(result, base_name)
        
        return paths
    
    def save_json(self, result: Dict[str, Any], base_name: str) -> Path:
        """Save complete result as JSON."""
        path = self.output_dir / f"{base_name}.json"
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, default=str)
        return path
    
    def save_excel(self, result: Dict[str, Any], base_name: str) -> Path:
        """Save result as Excel with flattened single-row structure."""
        if not EXCEL_AVAILABLE:
            raise ImportError("openpyxl required for Excel export. Install: pip install openpyxl")
        
        path = self.output_dir / f"{base_name}.xlsx"
        
        # Flatten
        row_data = self._flatten_result_to_single_row(result)
        
        # Write single row using common helper
        self._write_excel_file(path, [row_data], title="Results")
        return path

    def save_batch_report(self, results: List[Dict[str, Any]], base_name: str = "batch_report") -> Dict[str, Path]:
        """Save a batch of results into single consolidated CSV and Excel files."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_name = f"{base_name}_{timestamp}"
        paths = {}

        flattened_rows = [self._flatten_result_to_single_row(r) for r in results]
        
        if not flattened_rows:
            return paths

        # Save CSV
        csv_path = self.output_dir / f"{final_name}.csv"
        headers = list(flattened_rows[0].keys())
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(flattened_rows)
        paths['csv'] = csv_path

        # Save Excel
        if EXCEL_AVAILABLE:
            xlsx_path = self.output_dir / f"{final_name}.xlsx"
            self._write_excel_file(xlsx_path, flattened_rows, title="Batch Results")
            paths['excel'] = xlsx_path
            
        return paths

    def _write_excel_file(self, path: Path, rows: List[Dict[str, Any]], title: str = "Results"):
        """Helper to write a list of flattened dictionaries to an Excel file with styling."""
        wb = Workbook()
        ws = wb.active
        ws.title = title
        
        if not rows:
            wb.save(path)
            return

        headers = list(rows[0].keys())
        styles = self._get_excel_styles()
        
        # Write Header
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = styles['header_font']
            cell.fill = styles['header_fill']
            cell.border = styles['border']
        
        # Write Data
        for r_idx, row_data in enumerate(rows, 2):
            for c_idx, header in enumerate(headers, 1):
                val = row_data.get(header, '')
                cell = ws.cell(row=r_idx, column=c_idx, value=val)
                cell.alignment = styles['alignment']
                cell.border = styles['border']
                
                # Highlight issues
                if header == 'Overall Verdict' and val != 'PASS':
                    cell.fill = styles['warning_fill']
        
        # Adjust widths
        self._adjust_column_widths(ws, headers)
        
        wb.save(path)

    def _get_excel_styles(self) -> Dict[str, Any]:
        """Return common Excel styles."""
        return {
            'header_font': Font(bold=True, color="FFFFFF"),
            'header_fill': PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid"),
            'warning_fill': PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),
            'border': Border(left=Side(style='thin'), right=Side(style='thin'), 
                           top=Side(style='thin'), bottom=Side(style='thin')),
            'alignment': Alignment(wrap_text=True, vertical="top")
        }

    def _adjust_column_widths(self, ws, headers):
        """Set nice column widths based on header name."""
        for col, header in enumerate(headers, 1):
            width = 30
            if 'Copy' in header: width = 80
            if 'Fanouts' in header: width = 50
            if 'Logic' in header: width = 50
            ws.column_dimensions[chr(64+col) if col <= 26 else 'A'+chr(64+col-26)].width = width

    def save_csv(self, result: Dict[str, Any], base_name: str) -> Path:
        """Save result as flattened CSV (single row)."""
        path = self.output_dir / f"{base_name}.csv"
        
        row = self._flatten_result_to_single_row(result)
        
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(row.keys()))
            writer.writeheader()
            writer.writerow(row)
        
        return path

    def _flatten_result_to_single_row(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten entire result into a single dictionary (one row)."""
        fanout_out = result.get('fanout_output', {})
        validation = result.get('validation', {})
        
        # Format Fanouts List
        valid_fanouts = fanout_out.get('fanouts', [])
        formatted_fanouts = []
        for f in valid_fanouts:
            if isinstance(f, dict):
                formatted_fanouts.append(f"{f.get('query', '')} ({f.get('mapped_type', 'No Type')})")
            else:
                formatted_fanouts.append(str(f))
        
        # Format Rejected Fanouts
        rejected = fanout_out.get('rejected_fanouts', [])
        formatted_rejected = []
        for r in rejected:
            if isinstance(r, dict):
                formatted_rejected.append(f"{r.get('query', '')}: {r.get('reason', '')}")
            else:
                formatted_rejected.append(str(r))
                
        # Format Validation Steps
        val_steps = validation.get('steps', [])
        formatted_validation = []
        for step in val_steps:
            status = "✅" if step.get('is_valid') else "❌"
            formatted_validation.append(f"{status} {step.get('step')}: Score {step.get('score', 0)}")
            if step.get('errors'):
                formatted_validation.append(f"   Errors: {'; '.join(step['errors'])}")
            if step.get('warnings'):
                formatted_validation.append(f"   Warnings: {'; '.join(step['warnings'])}")
        
        return {
            "Main Query": result.get('query', ''),
            "Mode": result.get('mode', ''),
            "Valid Fanouts": "\n".join(formatted_fanouts),
            "Rejected Fanouts": "\n".join(formatted_rejected),
            "Must Cover": "\n".join(fanout_out.get('must_cover', [])),
            "Logic/Reasoning": fanout_out.get('reasoning', '') or fanout_out.get('validation_logic', '') or fanout_out.get('brainstorming_process', ''),
            "Copy Text": result.get('final_copy', ''),
            "Fanout Guardrail": result.get('fanout_guardrail_verdict', ''),
            "Copy Guardrail": result.get('copy_guardrail_verdict', ''),
            "Total Retries": result.get('fanout_retries', 0) + result.get('copy_retries', 0),
            "Quality Score": validation.get('overall_score', 0),
            "Validation Log": "\n".join(formatted_validation),
            "Time (s)": f"{result.get('total_time', 0):.2f}"
        }
