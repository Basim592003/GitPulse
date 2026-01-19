import streamlit as st
import pandas as pd
from io import BytesIO
import sys
sys.path.append(".")

st.set_page_config(
    page_title="GitPulse - GitHub Viral Predictor",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Space+Grotesk:wght@400;500;600;700&display=swap');
    
    .stApp {
        background: linear-gradient(135deg, #0a0a0f 0%, #1a1a2e 50%, #0f0f1a 100%);
        font-family: 'Space Grotesk', sans-serif;
    }
    
    .main-header {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #00d4ff, #7b2cbf);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    
    .sub-header {
        color: #888;
        font-size: 1rem;
        margin-bottom: 2rem;
    }
    
    .metric-card {
        background: linear-gradient(145deg, #1e1e2e, #2a2a3e);
        border: 1px solid #333;
        border-radius: 16px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
    }
    
    .metric-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2rem;
        font-weight: 700;
        color: #fff;
    }
    
    .metric-label {
        color: #888;
        font-size: 0.85rem;
        margin-top: 0.5rem;
    }
    
    .repo-card {
        background: linear-gradient(145deg, #1a1a2e, #252540);
        border: 1px solid #333;
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 1rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
    }
    
    .repo-card:hover {
        border-color: #7b2cbf;
        transform: translateY(-2px);
        box-shadow: 0 8px 30px rgba(123, 44, 191, 0.2);
    }
    
    .repo-name {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.1rem;
        font-weight: 600;
        color: #00d4ff;
        text-decoration: none;
    }
    
    .repo-stats {
        display: flex;
        gap: 1rem;
        margin: 0.75rem 0;
        color: #aaa;
        font-size: 0.9rem;
    }
    
    .prob-badge-viral {
        background: linear-gradient(90deg, #ff6b6b, #ee5a24);
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    
    .prob-badge-trending {
        background: linear-gradient(90deg, #4ecdc4, #44bd32);
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    
    .velocity-bar {
        height: 6px;
        background: #333;
        border-radius: 3px;
        overflow: hidden;
        margin-top: 0.5rem;
    }
    
    .velocity-fill {
        height: 100%;
        background: linear-gradient(90deg, #00d4ff, #7b2cbf);
        border-radius: 3px;
    }
    
    .sidebar .stSelectbox, .sidebar .stSlider {
        background: #1a1a2e;
    }
    
    div[data-testid="stHorizontalBlock"] {
        gap: 1rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)
def load_data():
    try:
        from ingest.config import get_s3_client, R2_BUCKET
        s3 = get_s3_client()
        response = s3.get_object(Bucket=R2_BUCKET, Key="predictions/latest.parquet")
        df = pd.read_parquet(BytesIO(response["Body"].read()))
        return df
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return None

df = load_data()
if df is None:
    st.warning("No prediction data available. Run `python ml/predict.py` first.")
    st.stop()

st.markdown('<p class="main-header">🚀 GitPulse</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Discover GitHub repos before they go viral</p>', unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### Filters")
    
    min_stars, max_stars = st.slider(
        "Stars Range", 
        min_value=0, 
        max_value=int(df["stars"].max()), 
        value=(0, int(df["stars"].max()))
    )
    
    min_forks, max_forks = st.slider(
        "Forks Range", 
        min_value=0, 
        max_value=int(df["forks"].max()), 
        value=(0, int(df["forks"].max()))
    )
    
    # prob_threshold = st.slider("Probability Threshold", 0.5, 0.99, 0.7)
    sort_by = st.selectbox("Sort By", ["Viral %", "Stars", "Star Velocity"])
    
    sort_map = {
        "Viral %": "viral_prob",
        # "Trending %": "trending_prob",
        "Stars": "stars",
        "Star Velocity": "star_velocity"
    }

filtered_df = df[
    (df["stars"] >= min_stars) & 
    (df["stars"] <= max_stars) &
    (df["forks"] >= min_forks) &
    (df["forks"] <= max_forks)
].copy()

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_analyzed = df["total_repos_analyzed"].iloc[0] if "total_repos_analyzed" in df.columns else len(df)
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{total_analyzed:,}</div>
        <div class="metric-label">Total Repos Analyzed</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    total_viral = df["total_predicted_viral"].iloc[0] if "total_predicted_viral" in df.columns else len(df[df["viral_prob"] >= 0.7])
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value" style="color: #ff6b6b;">{total_viral:,}</div>
        <div class="metric-label">Predicted Viral (≥70%)</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    if "trending_prob" in df.columns:
        trending_count = len(df[df["trending_prob"] >= 0.7])
    else:
        trending_count = 0
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value" style="color: #4ecdc4;">{trending_count:,}</div>
        <div class="metric-label">Predicted Trending(In Progress)</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    if "trending_prob" in df.columns:
        hot_count = len(df[(df["viral_prob"] >= 0.7) & (df["trending_prob"] >= 0.7)])
    else:
        hot_count = 0
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value" style="color: #ffd700;">{hot_count:,}</div>
        <div class="metric-label">Hot Repos(In Progress)</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["🔥 Viral", "📈 Trending", "⭐ Hot"])

def render_repo_card(row):
    viral_pct = int(row["viral_prob"] * 100)
    trending_pct = int(row.get("trending_prob", 0) * 100)
    velocity = row.get("star_velocity", 0)
    velocity_width = min(velocity * 10, 100)
    
    card_html = f"""
    <div class="repo-card">
        <a href="https://github.com/{row['repo_name']}" target="_blank" class="repo-name">{row['repo_name']}</a>
        <div class="repo-stats">
            <span>⭐ {int(row['stars']):,}</span>
            <span>🍴 {int(row.get('forks', 0)):,}</span>
        </div>
        <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 0.5rem;">
            <span style="color: #888; font-size: 0.85rem;">Star Velocity</span>
            <span style="color: #fff; font-weight: 600;">{velocity:.1f}x normal</span>
        </div>
        <div class="velocity-bar">
            <div class="velocity-fill" style="width: {velocity_width}%;"></div>
        </div>
        <div style="display: flex; gap: 0.5rem; margin-top: 1rem;">
            <span class="prob-badge-viral">🔥 Viral {viral_pct}%</span>
            <span class="prob-badge-trending">📈 Trending {trending_pct}%</span>
        </div>
    </div>
    """
    return card_html

with tab1:
    viral_df = filtered_df[filtered_df["viral_prob"] >= prob_threshold].sort_values(
        sort_map[sort_by], ascending=False
    ).head(50)
    
    if len(viral_df) == 0:
        st.info("No repos match the current filters.")
    else:
        cols = st.columns(3)
        for idx, (_, row) in enumerate(viral_df.iterrows()):
            with cols[idx % 3]:
                st.markdown(render_repo_card(row), unsafe_allow_html=True)

with tab2:
    if "trending_prob" in filtered_df.columns:
        trending_df = filtered_df[filtered_df["trending_prob"] >= prob_threshold].sort_values(
            "trending_prob", ascending=False
        ).head(50)
        
        if len(trending_df) == 0:
            st.info("No repos match the current filters.")
        else:
            cols = st.columns(3)
            for idx, (_, row) in enumerate(trending_df.iterrows()):
                with cols[idx % 3]:
                    st.markdown(render_repo_card(row), unsafe_allow_html=True)
    else:
        st.info("Coming Soon.")

with tab3:
    if "trending_prob" in filtered_df.columns:
        hot_df = filtered_df[
            (filtered_df["viral_prob"] >= prob_threshold) & 
            (filtered_df["trending_prob"] >= prob_threshold)
        ].sort_values("viral_prob", ascending=False).head(50)
        
        if len(hot_df) == 0:
            st.info("No repos are both viral and trending at current threshold.")
        else:
            cols = st.columns(3)
            for idx, (_, row) in enumerate(hot_df.iterrows()):
                with cols[idx % 3]:
                    st.markdown(render_repo_card(row), unsafe_allow_html=True)
    else:
        st.info("Coming Soon.")

st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown(
    "<p style='text-align: center; color: #555;'>GitPulse • Predicting GitHub trends with ML • Updated daily</p>", 
    unsafe_allow_html=True
)
