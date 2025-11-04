import os
import io
import json
import base64
import streamlit as st
from databricks.sdk import WorkspaceClient

databricks_host = os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_HOSTNAME")
w = WorkspaceClient()

st.header(body="PDF Field Extractor", divider=True)
st.write("Upload a PDF, extract fields with LLM, review, and save the results.")

# Initialize session state
if "uploaded_pdf_bytes" not in st.session_state:
    st.session_state.uploaded_pdf_bytes = None
if "uploaded_pdf_name" not in st.session_state:
    st.session_state.uploaded_pdf_name = None
if "uploaded_pdf_path" not in st.session_state:
    st.session_state.uploaded_pdf_path = None
if "prompt_results" not in st.session_state:
    st.session_state.prompt_results = {}
if "final_json" not in st.session_state:
    st.session_state.final_json = {}

def pdf_to_base64(pdf_bytes):
    """Convert PDF bytes to base64 string for LLM"""
    return base64.b64encode(pdf_bytes).decode('utf-8')

def extract_with_llm(pdf_base64, prompt):
    """Call databricks-llama-4-maverick with PDF and prompt"""
    try:
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

        # Get OpenAI-compatible client
        client = w.serving_endpoints.get_open_ai_client()

        response = client.chat.completions.create(
            model="databricks-llama-4-maverick",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:application/pdf;base64,{pdf_base64}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=1000
        )

        return response.choices[0].message.content
    except Exception as e:
        st.error(f"LLM error: {e}")
        return None

# STEP 1: Upload PDF
st.subheader("Step 1: Upload PDF")
uploaded_file = st.file_uploader("Select PDF file", type=["pdf"])

if uploaded_file:
    # Store file in session
    st.session_state.uploaded_pdf_bytes = uploaded_file.read()
    st.session_state.uploaded_pdf_name = uploaded_file.name
    st.success(f"✅ File loaded: {uploaded_file.name}")

    if st.button("Save to Volume (jywu.multimodal.test)"):
        try:
            binary_data = io.BytesIO(st.session_state.uploaded_pdf_bytes)
            volume_file_path = f"/Volumes/jywu/multimodal/test/{st.session_state.uploaded_pdf_name}"
            w.files.upload(volume_file_path, binary_data, overwrite=True)
            st.session_state.uploaded_pdf_path = volume_file_path
            volume_url = f"https://{databricks_host}/explore/data/volumes/jywu/multimodal/test"
            st.success(f"✅ Saved to volume: `{volume_file_path}` [Go to volume]({volume_url})")
        except Exception as e:
            st.error(f"Error: {e}")

# STEP 2: Extract with multiple prompts
if st.session_state.uploaded_pdf_bytes:
    st.divider()
    st.subheader("Step 2: Extract Fields with LLM")

    # Define prompts
    prompts = {
        "Prompt 1": "Can you extract total number of holes? output a json",
        "Prompt 2": "Can you extract dimension info on top of section C-C section? output a json"
    }

    # Show prompts
    for name, prompt in prompts.items():
        st.info(f"**{name}:** {prompt}")

    if st.button("Invoke All Prompts"):
        with st.spinner("🔄 Calling databricks-llama-4-maverick..."):
            pdf_b64 = pdf_to_base64(st.session_state.uploaded_pdf_bytes)
            st.session_state.prompt_results = {}

            for name, prompt in prompts.items():
                st.write(f"Processing {name}...")
                response = extract_with_llm(pdf_b64, prompt)
                st.session_state.prompt_results[name] = response

            st.success("✅ All extractions complete!")

    # Show all results
    if st.session_state.prompt_results:
        st.divider()
        st.subheader("Extraction Results")
        for name, result in st.session_state.prompt_results.items():
            with st.expander(f"{name} - Result", expanded=True):
                st.json(result)

        # STEP 3: Combine JSONs
        st.divider()
        if st.button("Combine All JSONs"):
            combined = {}
            for name, result in st.session_state.prompt_results.items():
                try:
                    # Try to parse as JSON
                    if isinstance(result, str):
                        parsed = json.loads(result)
                    else:
                        parsed = result
                    combined[name] = parsed
                except:
                    # If not valid JSON, keep as string
                    combined[name] = result

            st.session_state.final_json = combined
            st.success("✅ Combined all results!")
            st.json(combined)

# STEP 4: Edit and Save
if st.session_state.final_json:
    st.divider()
    st.subheader("Step 3: Review & Edit")

    edited_json = st.text_area(
        "Edit combined JSON:",
        value=json.dumps(st.session_state.final_json, indent=2),
        height=300
    )

    if st.button("Validate & Update"):
        try:
            st.session_state.final_json = json.loads(edited_json)
            st.success("✅ Valid JSON updated!")
        except Exception as e:
            st.error(f"Invalid JSON: {e}")

    st.divider()
    st.subheader("Step 4: Save Results")

    # Auto-generate filename
    default_name = "extracted_fields.json"
    if st.session_state.uploaded_pdf_name:
        base_name = st.session_state.uploaded_pdf_name.replace(".pdf", "")
        default_name = f"{base_name}_extracted.json"

    output_filename = st.text_input("Output filename:", value=default_name)

    if st.button("Save JSON to Volume"):
        try:
            json_str = json.dumps(st.session_state.final_json, indent=2)
            json_bytes = io.BytesIO(json_str.encode('utf-8'))
            output_path = f"/Volumes/jywu/multimodal/test/{output_filename}"
            w.files.upload(output_path, json_bytes, overwrite=True)
            volume_url = f"https://{databricks_host}/explore/data/volumes/jywu/multimodal/test"
            st.success(f"✅ Saved to: `{output_path}` [Go to volume]({volume_url})")

            # Download button
            st.download_button(
                label="Download JSON",
                data=json_str,
                file_name=output_filename,
                mime="application/json"
            )
        except Exception as e:
            st.error(f"Error: {e}")
