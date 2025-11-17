using UnityEngine;
using UnityEngine.UI;

/// <summary>
/// AppFig A/B Testing Component
///
/// Specialized component for A/B testing and experimentation with unlimited variants.
/// Maps feature flag values to Unity assets (sprites, materials, prefabs, colors, etc.)
///
/// REQUIREMENTS:
/// - AppFig.Init() must be called before using this component (e.g., in a startup script)
/// - Does NOT require AppFigHelper component
///
/// WHEN TO USE:
/// - Testing different visual assets (sprites, materials, colors)
/// - Experimenting with multiple game variants (3+ options)
/// - A/B/C/D testing scenarios
/// - Swapping entire prefabs based on feature flags
///
/// WHEN TO USE AppFigApplier INSTEAD:
/// - Simple show/hide logic (use EnableDisable type)
/// - Displaying text values (use TextContent type)
/// - Settings numeric values (use NumericValue type)
///
/// EXAMPLES:
///
/// Example 1: Character Skin Testing
///   Feature: "character_skin" with values "default", "premium", "seasonal"
///   Variants: Map each value to a different sprite asset
///
/// Example 2: Button Color A/B Test
///   Feature: "button_color" with values "red", "blue", "green"
///   Variants: Map each value to a different color
///
/// Example 3: UI Layout Testing
///   Feature: "ui_layout" with values "classic", "modern", "minimal"
///   Variants: Map each value to different prefab instances
/// </summary>
public class AppFigABTester : MonoBehaviour
{
    #region Enums

    /// <summary>
    /// Types of components that can be swapped for A/B testing
    /// </summary>
    public enum ComponentType
    {
        Sprite,      // Image or SpriteRenderer
        Material,    // Renderer materials
        Color,       // Image or SpriteRenderer color
        Prefab,      // Instantiate different prefabs
        AudioClip    // AudioSource clip
    }

    #endregion

    #region Serialized Fields

    [Header("Feature Configuration")]
    [Tooltip("Name of the feature flag to watch (e.g., 'character_skin', 'button_color')")]
    [SerializeField] private string featureName;

    [Header("Component Configuration")]
    [Tooltip("Type of component to swap based on feature value")]
    [SerializeField] private ComponentType componentType = ComponentType.Sprite;

    [Header("Variant Mappings")]
    [Tooltip("Map feature values to assets. Feature value must match exactly (case-insensitive).")]
    [SerializeField] private VariantMapping[] variants;

    [Tooltip("Optional fallback if feature value doesn't match any variant")]
    [SerializeField] private VariantMapping fallbackVariant;

    [Header("Behavior")]
    [Tooltip("Apply variant immediately on Start()")]
    [SerializeField] private bool applyOnStart = true;

    #endregion

    #region Private Fields

    private string lastFeatureValue = null;

    // Cached component references
    private Image imageComponent;
    private SpriteRenderer spriteRenderer;
    private Renderer rendererComponent;
    private AudioSource audioSource;
    private GameObject currentPrefabInstance;

    #endregion

    #region Unity Lifecycle

    private void Start()
    {
        CacheComponents();

        if (applyOnStart)
        {
            ApplyVariant();
        }
    }

    private void Update()
    {
        // Check if feature value has changed (direct SDK access)
        string currentValue = AppFig.GetFeatureValue(featureName);

        if (currentValue != lastFeatureValue)
        {
            lastFeatureValue = currentValue;
            ApplyVariant();
        }
    }

    #endregion

    #region Component Caching

    /// <summary>
    /// Cache component references based on component type
    /// </summary>
    private void CacheComponents()
    {
        switch (componentType)
        {
            case ComponentType.Sprite:
            case ComponentType.Color:
                imageComponent = GetComponent<Image>();
                if (imageComponent == null)
                {
                    spriteRenderer = GetComponent<SpriteRenderer>();
                }
                break;

            case ComponentType.Material:
                rendererComponent = GetComponent<Renderer>();
                break;

            case ComponentType.AudioClip:
                audioSource = GetComponent<AudioSource>();
                break;

            case ComponentType.Prefab:
                // No caching needed for prefabs
                break;
        }
    }

    #endregion

    #region Variant Application

    /// <summary>
    /// Apply the variant that matches the current feature value
    /// </summary>
    public void ApplyVariant()
    {
        // Get feature value directly from AppFig SDK
        string featureValue = AppFig.GetFeatureValue(featureName);

        if (string.IsNullOrEmpty(featureValue))
        {
            Debug.LogWarning($"[AppFigABTester] Feature '{featureName}' has no value");
            return;
        }

        // Find matching variant (case-insensitive)
        VariantMapping matchedVariant = FindMatchingVariant(featureValue);

        if (matchedVariant == null)
        {
            // Use fallback if no match found
            if (fallbackVariant != null && !string.IsNullOrEmpty(fallbackVariant.featureValue))
            {
                matchedVariant = fallbackVariant;
                Debug.Log($"[AppFigABTester] No match for '{featureValue}', using fallback variant");
            }
            else
            {
                Debug.LogWarning($"[AppFigABTester] No variant found for value '{featureValue}' and no fallback defined");
                return;
            }
        }

        // Apply the matched variant based on component type
        ApplyVariantByType(matchedVariant);
    }

    /// <summary>
    /// Find variant that matches the feature value
    /// </summary>
    private VariantMapping FindMatchingVariant(string featureValue)
    {
        if (variants == null || variants.Length == 0)
        {
            return null;
        }

        string normalizedValue = featureValue.ToLower().Trim();

        foreach (var variant in variants)
        {
            if (!string.IsNullOrEmpty(variant.featureValue))
            {
                string normalizedVariantValue = variant.featureValue.ToLower().Trim();
                if (normalizedVariantValue == normalizedValue)
                {
                    return variant;
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Apply variant based on component type
    /// </summary>
    private void ApplyVariantByType(VariantMapping variant)
    {
        switch (componentType)
        {
            case ComponentType.Sprite:
                ApplySpriteVariant(variant);
                break;

            case ComponentType.Material:
                ApplyMaterialVariant(variant);
                break;

            case ComponentType.Color:
                ApplyColorVariant(variant);
                break;

            case ComponentType.Prefab:
                ApplyPrefabVariant(variant);
                break;

            case ComponentType.AudioClip:
                ApplyAudioClipVariant(variant);
                break;
        }
    }

    /// <summary>
    /// Apply sprite variant to Image or SpriteRenderer
    /// </summary>
    private void ApplySpriteVariant(VariantMapping variant)
    {
        if (variant.sprite == null)
        {
            Debug.LogWarning($"[AppFigABTester] Variant '{variant.featureValue}' has no sprite assigned");
            return;
        }

        if (imageComponent != null)
        {
            imageComponent.sprite = variant.sprite;
        }
        else if (spriteRenderer != null)
        {
            spriteRenderer.sprite = variant.sprite;
        }
        else
        {
            Debug.LogWarning("[AppFigABTester] No Image or SpriteRenderer component found");
        }
    }

    /// <summary>
    /// Apply material variant to Renderer
    /// </summary>
    private void ApplyMaterialVariant(VariantMapping variant)
    {
        if (variant.material == null)
        {
            Debug.LogWarning($"[AppFigABTester] Variant '{variant.featureValue}' has no material assigned");
            return;
        }

        if (rendererComponent != null)
        {
            rendererComponent.material = variant.material;
        }
        else
        {
            Debug.LogWarning("[AppFigABTester] No Renderer component found");
        }
    }

    /// <summary>
    /// Apply color variant to Image or SpriteRenderer
    /// </summary>
    private void ApplyColorVariant(VariantMapping variant)
    {
        if (imageComponent != null)
        {
            imageComponent.color = variant.color;
        }
        else if (spriteRenderer != null)
        {
            spriteRenderer.color = variant.color;
        }
        else
        {
            Debug.LogWarning("[AppFigABTester] No Image or SpriteRenderer component found");
        }
    }

    /// <summary>
    /// Apply prefab variant by instantiating as child
    /// </summary>
    private void ApplyPrefabVariant(VariantMapping variant)
    {
        if (variant.prefab == null)
        {
            Debug.LogWarning($"[AppFigABTester] Variant '{variant.featureValue}' has no prefab assigned");
            return;
        }

        // Destroy previous instance if it exists
        if (currentPrefabInstance != null)
        {
            Destroy(currentPrefabInstance);
        }

        // Instantiate new prefab as child
        currentPrefabInstance = Instantiate(variant.prefab, transform);
        currentPrefabInstance.transform.localPosition = Vector3.zero;
        currentPrefabInstance.transform.localRotation = Quaternion.identity;
        currentPrefabInstance.transform.localScale = Vector3.one;
    }

    /// <summary>
    /// Apply audio clip variant to AudioSource
    /// </summary>
    private void ApplyAudioClipVariant(VariantMapping variant)
    {
        if (variant.audioClip == null)
        {
            Debug.LogWarning($"[AppFigABTester] Variant '{variant.featureValue}' has no audio clip assigned");
            return;
        }

        if (audioSource != null)
        {
            audioSource.clip = variant.audioClip;
        }
        else
        {
            Debug.LogWarning("[AppFigABTester] No AudioSource component found");
        }
    }

    #endregion

    #region Public API

    /// <summary>
    /// Force reapplication of the current variant
    /// </summary>
    public void ForceUpdate()
    {
        lastFeatureValue = null; // Reset to force update
        ApplyVariant();
    }

    #endregion
}

#region Variant Mapping

/// <summary>
/// Maps a feature value to Unity assets for A/B testing
/// </summary>
[System.Serializable]
public class VariantMapping
{
    [Tooltip("Feature value that triggers this variant (e.g., 'black', 'white', 'version_a')")]
    public string featureValue;

    [Header("Assets (assign based on Component Type)")]
    [Tooltip("Sprite for ComponentType.Sprite")]
    public Sprite sprite;

    [Tooltip("Material for ComponentType.Material")]
    public Material material;

    [Tooltip("Color for ComponentType.Color")]
    public Color color = Color.white;

    [Tooltip("Prefab for ComponentType.Prefab")]
    public GameObject prefab;

    [Tooltip("AudioClip for ComponentType.AudioClip")]
    public AudioClip audioClip;
}

#endregion
