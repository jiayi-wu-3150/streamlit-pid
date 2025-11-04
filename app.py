import io
import json
import base64
import streamlit as st
from databricks.sdk import WorkspaceClient

st.set_page_config(layout="wide", page_title="Image Field Extractor")

w = WorkspaceClient()
# Get the host from workspace client config
databricks_host = w.config.host.replace("https://", "") if w.config.host else ""

st.header(body="Image Field Extractor", divider=True)
st.write("Upload an image, extract fields with LLM, review, and save the results.")

# Initialize session state
if "uploaded_image_bytes" not in st.session_state:
    st.session_state.uploaded_image_bytes = None
if "uploaded_image_name" not in st.session_state:
    st.session_state.uploaded_image_name = None
if "uploaded_image_path" not in st.session_state:
    st.session_state.uploaded_image_path = None
if "prompt_results" not in st.session_state:
    st.session_state.prompt_results = {}
if "final_json" not in st.session_state:
    st.session_state.final_json = {}

def image_to_base64(image_bytes):
    """Convert image bytes to base64 string for LLM"""
    return base64.b64encode(image_bytes).decode('utf-8')

def extract_with_llm(image_base64, prompt):
    """Call databricks-llama-4-maverick with image and prompt"""
    try:
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
                                "url": f"data:image/jpeg;base64,{image_base64}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=5000,
            temperature=0
        )

        return response.choices[0].message.content
    except Exception as e:
        st.error(f"LLM error: {e}")
        return None

# STEP 1: Upload Image
st.subheader("Step 1: Upload Image")
uploaded_file = st.file_uploader("Select JPG/PNG file", type=["jpg", "jpeg", "png"])

if uploaded_file:
    # Store file in session
    st.session_state.uploaded_image_bytes = uploaded_file.read()
    st.session_state.uploaded_image_name = uploaded_file.name
    st.success(f"✅ File loaded: {uploaded_file.name}")

    # Show image at full resolution
    from PIL import Image
    img = Image.open(io.BytesIO(st.session_state.uploaded_image_bytes))
    st.image(img, caption="Uploaded Image", output_format="PNG")

    if st.button("Save to Volume (jywu.multimodal.test)"):
        try:
            binary_data = io.BytesIO(st.session_state.uploaded_image_bytes)
            volume_file_path = f"/Volumes/jywu/multimodal/test/{st.session_state.uploaded_image_name}"
            w.files.upload(volume_file_path, binary_data, overwrite=True)
            st.session_state.uploaded_image_path = volume_file_path
            volume_url = f"https://{databricks_host}/explore/data/volumes/jywu/multimodal/test"
            st.success(f"✅ Saved to volume: `{volume_file_path}` [Go to volume]({volume_url})")
        except Exception as e:
            st.error(f"Error: {e}")

# STEP 2: Extract with multiple prompts
if st.session_state.uploaded_image_bytes:
    st.divider()
    st.subheader("Step 2: Extract Fields with LLM")

    # Define prompts
    prompts = {
        "total_number_of_holes": "can you extract total number of holes? output a json",
        "dowel_holds": "can you extract dowel holds table on the right upper coner? Note that there might be merged rows for some columns. output a json only.",
        "drill_dimensions": "can you extract dimension info on top of section C-C section? output a json"
    }

    # Show prompts
    for name, prompt in prompts.items():
        st.info(f"**{name}:** {prompt}")

    if st.button("Invoke All Prompts"):
        with st.spinner("🔄 Calling databricks-llama-4-maverick..."):
            image_b64 = image_to_base64(st.session_state.uploaded_image_bytes)
            st.session_state.prompt_results = {}

            for name, prompt in prompts.items():
                st.write(f"Processing {name}...")
                response = extract_with_llm(image_b64, prompt)
                st.session_state.prompt_results[name] = response

            st.success("✅ All extractions complete!")

    # Show all results
    if st.session_state.prompt_results:
        st.divider()
        st.subheader("Extraction Results")
        for name, result in st.session_state.prompt_results.items():
            with st.expander(f"{name} - Result", expanded=True):
                # Show raw result as text/code
                if isinstance(result, str):
                    st.code(result, language="json")
                else:
                    st.json(result)

        # STEP 3: Combine JSONs
        st.divider()
        if st.button("Combine All JSONs"):
            combined = {}
            for name, result in st.session_state.prompt_results.items():
                try:
                    # Try to parse as JSON
                    if isinstance(result, str):
                        # Remove markdown code blocks if present
                        cleaned = result.strip()
                        if cleaned.startswith("```json"):
                            cleaned = cleaned[7:]  # Remove ```json
                        if cleaned.startswith("```"):
                            cleaned = cleaned[3:]  # Remove ```
                        if cleaned.endswith("```"):
                            cleaned = cleaned[:-3]  # Remove trailing ```
                        cleaned = cleaned.strip()

                        parsed = json.loads(cleaned)
                    else:
                        parsed = result
                    combined[name] = parsed
                except Exception as e:
                    # If not valid JSON, keep as string
                    st.warning(f"{name} parse error: {e}. Keeping as string.")
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
    if st.session_state.uploaded_image_name:
        base_name = st.session_state.uploaded_image_name.rsplit(".", 1)[0]
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
